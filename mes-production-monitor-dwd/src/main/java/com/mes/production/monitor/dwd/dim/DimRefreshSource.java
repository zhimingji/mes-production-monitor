package com.mes.production.monitor.dwd.dim;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 维表定期全量刷新源，作为广播流输入。
 *
 * <p>维表变化少（新增订单、偶尔加产线），定期全量刷新足够，比给 6 张维表都配 CDC 简单得多。
 * 代价是维表变更有最长一个刷新周期的延迟——demo 可接受，简历里如实说明「维表采用定期刷新 + 广播」。
 *
 * <p>并行度必须设为 1：多个并行子任务同时全量查库没有意义，广播下去还会互相覆盖。
 */
public class DimRefreshSource extends RichSourceFunction<DimCache> {

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
    public void run(SourceContext<DimCache> ctx) throws Exception {
        while (running) {
            try {
                DimCache cache = DimLoader.loadAll(jdbcUrlTemplate, username, password);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(cache);
                }
            } catch (Exception e) {
                // 刷新失败不能让作业挂掉：下游还有 open() 里 bootstrap 的那份缓存兜底
                LOG.error("维表刷新失败，本轮跳过，下游继续使用上一版缓存", e);
            }
            Thread.sleep(refreshIntervalMs);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
