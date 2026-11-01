package com.mes.production.monitor.dwd.dim;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 静态维表定期全量刷新源，作为广播流输入（只刷 3 张静态码表，不含订单、不含 part）
 *
 * <p>workcenter/worksite/user 这几张码表几乎不变（偶尔新增），定期全量刷新足够，
 * 比给它们都配 CDC 简单得多。代价是变更有最长一个刷新周期的延迟，可接受。
 * part 是 20 万/基地的大维表，不走这里，见 {@link PartLookup}。
 *
 * <p>订单维（productionorder）不在这里刷：它会 UPDATE（订单量/交期修正），影响达成率与告警，
 * 必须走 CDC 增量广播，见 {@link OrderCdcParser} 与文档 6.2.4。两类更新 union 成同一个广播流，
 * 由 {@link DimUpdate} 的 {@code kind} 区分。
 *
 * <p>并行度必须设为 1：多个并行子任务同时全量查库没有意义，广播下去还会互相覆盖
 */
public class DimRefreshSource extends RichSourceFunction<DimUpdate> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DimRefreshSource.class);

    private final String jdbcUrlTemplate;
    private final String username;
    private final String password;
    private final long refreshIntervalMs;

    private volatile boolean running = true;

    public DimRefreshSource(String jdbcUrlTemplate, String username, String password, long refreshIntervalMs) {
        this.jdbcUrlTemplate = jdbcUrlTemplate;
        this.username = username;
        this.password = password;
        this.refreshIntervalMs = refreshIntervalMs;
    }

    @Override
    public void run(SourceContext<DimUpdate> ctx) throws Exception {
        while (running) {
            try {
                // 只刷静态维表；订单维由 OrderCdcParser 消费 ods-mes-order 增量维护
                DimCache cache = DimLoader.loadStatic(jdbcUrlTemplate, username, password);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(DimUpdate.staticSnapshot(cache));
                }
            } catch (Exception e) {
                // 刷新失败不能让作业挂掉：下游还有 open() 里 bootstrap 的那份缓存兜底
                LOG.error("静态维表刷新失败，本轮跳过，下游继续使用上一版缓存", e);
            }
            Thread.sleep(refreshIntervalMs);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
