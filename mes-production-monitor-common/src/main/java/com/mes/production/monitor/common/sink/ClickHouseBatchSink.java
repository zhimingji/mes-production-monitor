package com.mes.production.monitor.common.sink;

import com.mes.production.monitor.common.metric.MetricNames;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ClickHouse 攒批 Sink。见文档 11.2。
 *
 * <h3>为什么必须攒批</h3>
 * ClickHouse <b>每次 INSERT 都会生成一个 data part</b>。Flink 逐条或小批量写入会让 part 数量迅速累积，
 * 超过 {@code parts_to_throw_insert} 后直接拒绝写入：
 * <pre>
 * DB::Exception: Too many parts (N). Merges are processing significantly slower than inserts
 * </pre>
 *
 * <h3>为什么是条数 + 时间双触发</h3>
 * 单一条件都不行：只按条数，低峰期数据会长时间不落地（实时性没了）；
 * 只按时间，高峰期批次会过大（内存和单次写入耗时失控）。
 *
 * <h3>语义边界</h3>
 * 实现 {@link CheckpointedFunction}，在 {@code snapshotState} 里强制 flush 缓冲区。
 * 这保证了 <b>at-least-once</b>：作业失败重启会重放，可能重复写入，
 * 靠 ClickHouse 侧 {@code ReplacingMergeTree(version)} + 业务唯一键做幂等去重达到最终一致。
 * 这就是文档 10.1 说的「exactly-once 的边界」——不要笼统宣称全链路 exactly-once。
 *
 * <h3>子类需要实现什么</h3>
 * <ul>
 *   <li>{@link #insertSql()}：带占位符的 INSERT 语句</li>
 *   <li>{@link #bindRow(PreparedStatement, Object)}：把一条记录绑定到占位符</li>
 * </ul>
 *
 * @param <T> 写入的记录类型
 */
public abstract class ClickHouseBatchSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient List<T> buffer;
    private transient ScheduledExecutorService scheduler;
    private transient volatile Exception asyncFlushError;

    private transient Counter writtenRows;
    private transient volatile long lastFlushDurationMs;

    /**
     * @param jdbcUrl         如 {@code jdbc:ch://localhost:8123/mes_production_monitor}
     * @param batchSize       攒批条数阈值，建议 5000
     * @param flushIntervalMs 攒批时间阈值（毫秒），建议 10000
     */
    protected ClickHouseBatchSink(String jdbcUrl, String username, String password,
                                  int batchSize, long flushIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    /** 带占位符的 INSERT 语句，如 {@code INSERT INTO dwd_mes_report (a, b) VALUES (?, ?)} */
    protected abstract String insertSql();

    /** 把一条记录绑定到 PreparedStatement 的占位符上，参数下标与 {@link #insertSql()} 一致 */
    protected abstract void bindRow(PreparedStatement ps, T value) throws SQLException;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.buffer = new ArrayList<>(batchSize);

        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        this.connection.setAutoCommit(true);
        this.statement = connection.prepareStatement(insertSql());

        MetricGroup group = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
        this.writtenRows = group.counter(MetricNames.CK_WRITTEN_ROWS);
        group.gauge(MetricNames.CK_FLUSH_DURATION_MS, (Gauge<Long>) () -> lastFlushDurationMs);

        if (flushIntervalMs > 0) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ck-batch-flush");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    synchronized (ClickHouseBatchSink.this) {
                        flush();
                    }
                } catch (Exception e) {
                    // 定时线程里的异常必须记下来，在主线程抛出，否则会被静默吞掉
                    asyncFlushError = e;
                    LOG.error("定时 flush ClickHouse 失败", e);
                }
            }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        }

        LOG.info("ClickHouseBatchSink 初始化完成: url={}, batchSize={}, flushIntervalMs={}",
                jdbcUrl, batchSize, flushIntervalMs);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkAsyncError();
        synchronized (this) {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush();
            }
        }
    }

    /** checkpoint 时强制 flush，否则失败重启会丢掉缓冲区里的数据 */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkAsyncError();
        synchronized (this) {
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // 缓冲区不进状态：语义是 at-least-once，重启后由上游重放补齐，
        // 依赖 ClickHouse 的 ReplacingMergeTree 幂等去重。
    }

    /** 调用方必须持有本对象的锁 */
    private void flush() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();
        for (T value : buffer) {
            bindRow(statement, value);
            statement.addBatch();
        }
        statement.executeBatch();
        statement.clearBatch();

        int flushed = buffer.size();
        buffer.clear();
        lastFlushDurationMs = System.currentTimeMillis() - start;
        writtenRows.inc(flushed);

        if (LOG.isDebugEnabled()) {
            LOG.debug("flush {} 行到 ClickHouse，耗时 {} ms", flushed, lastFlushDurationMs);
        }
    }

    private void checkAsyncError() throws Exception {
        Exception e = asyncFlushError;
        if (e != null) {
            asyncFlushError = null;
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        try {
            synchronized (this) {
                if (buffer != null) {
                    flush();
                }
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        super.close();
    }
}
