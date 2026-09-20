package com.mes.production.monitor.dwd;

import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.dwd.dim.DimCache;
import com.mes.production.monitor.dwd.dim.DimRefreshSource;
import com.mes.production.monitor.dwd.function.DimBroadcastJoinFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业 2：ODS -&gt; DWD 明细宽表。见文档 6.2。
 *
 * <pre>
 * Kafka ods-mes-report (debezium-json)
 *        │
 *        ├── 解析成 OdsReportRecord（两通道统一 POJO）
 *        │
 *        ├── connect(维表广播流) ── DimBroadcastJoinFunction
 *        │      订单关联 + 维表关联 + 口径过滤（本层不聚合）
 *        │
 *        ├──> Kafka dwd-mes-report   （供 DWS / 告警作业消费）
 *        └──> ClickHouse dwd_mes_report（明细查询 + 对账）
 * </pre>
 */
public class DwdWideTableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwdWideTableJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("dwd.properties", args);
        StreamExecutionEnvironment env = FlinkEnvFactory.create(conf);

        // ---------- 维表广播流 ----------
        // 并行度必须是 1：多个子任务同时全量查库没意义，广播下去还会互相覆盖。
        DataStream<DimCache> dimStream = env
                .addSource(new DimRefreshSource(
                        conf.getString("mysql.jdbc.url.template"),
                        conf.getString("mysql.username"),
                        conf.getString("mysql.password"),
                        conf.getLong("dim.refresh.interval.ms", 300_000L)))
                .name("dim-refresh-source")
                .setParallelism(1);

        BroadcastStream<DimCache> dimBroadcast =
                dimStream.broadcast(DimBroadcastJoinFunction.DIM_STATE_DESCRIPTOR);

        // TODO(M3-2) 建 Kafka source 读 ods-mes-report，解析成 OdsReportRecord。
        //
        //   要点 1：ODS topic 里是 debezium-json 信封，结构是
        //     { "before": {...}, "after": {...}, "source": {"db":"...", "table":"...", "ts_ms":...},
        //       "op": "c|u|d|r", "ts_ms": ... }
        //   用 JsonUtil.readTree() 解析，从 source.db 拿库名，从 op 拿变更类型。
        //   这一步不要用 Flink SQL 的 debezium-json format——那样拿不到 op 字段做显式判断，
        //   而"能显式看到 -U 旧值"正是选路线 B 的意义所在（文档 6.1.1）。
        //
        //   要点 2：watermark 用事件时间 + 60s 乱序容忍：
        //     WatermarkStrategy.<OdsReportRecord>forBoundedOutOfOrderness(Duration.ofSeconds(60))
        //         .withTimestampAssigner((r, ts) -> r.eventTime())
        //         .withIdleness(Duration.ofMinutes(1))   // 空闲分区不阻塞全局 watermark
        //
        //   要点 3：解析失败不要抛异常让作业挂掉，计入 dirty_record_count 后丢弃。
        //
        // DataStream<OdsReportRecord> odsStream = env.fromSource(...)
        //         .name("kafka-ods-source")
        //         .uid("kafka-ods-source");   // uid 一定要设，否则改代码后无法从 savepoint 恢复

        // TODO(M3-3) connect 广播流做维表 join
        // DataStream<DwdReportRecord> dwdStream = odsStream
        //         .connect(dimBroadcast)
        //         .process(new DimBroadcastJoinFunction(
        //                 conf.getString("mysql.jdbc.url.template"),
        //                 conf.getString("mysql.username"),
        //                 conf.getString("mysql.password")))
        //         .name("dim-broadcast-join")
        //         .uid("dim-broadcast-join");

        // TODO(M3-4) 双 sink
        //   1. Kafka dwd-mes-report：分区键用 productionorder 的 hash。
        //      别用 "company:datasource:productionorder" 这种带前缀的 key——
        //      company 前缀会让分区分布跟着基地规模倾斜（文档 11.1）。
        //   2. ClickHouse：dwdStream.addSink(new DwdReportCkSink(...))
        //      攒批参数从配置读，不要写死。
        //
        // dwdStream.sinkTo(kafkaSink).name("kafka-dwd-sink").uid("kafka-dwd-sink");
        // dwdStream.addSink(new DwdReportCkSink(
        //         conf.getString("clickhouse.jdbc.url"),
        //         conf.getString("clickhouse.username"),
        //         conf.getString("clickhouse.password"),
        //         conf.getInt("clickhouse.batch.size", 5000),
        //         conf.getLong("clickhouse.flush.interval.ms", 10_000L)))
        //         .name("ck-dwd-sink").uid("ck-dwd-sink");

        LOG.warn("DWD 作业骨架尚未完成，请先实现 M3-2 / M3-3 / M3-4 的 TODO");
        // env.execute("mes-production-monitor-dwd-wide-table");
    }
}
