package com.mes.production.monitor.dws;

import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业 3：DWD -&gt; DWS 指标聚合。见文档 6.3。
 *
 * <pre>
 * Kafka dwd-mes-report
 *        │
 *        ├── 1min 滚动窗口聚合 ──────> ClickHouse dws_report_1min
 *        └── keyBy(订单) 累计达成率 ──> ClickHouse dws_order_achievement
 * </pre>
 *
 * <p><b>两个粒度放在同一个作业里</b>，用多路 sink 输出，不要拆成两个作业——
 * 两个作业会重复消费同一 topic、重复解析，浪费资源且口径容易漂移。
 */
public class DwsMetricsJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwsMetricsJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("dws.properties", args);
        StreamExecutionEnvironment env = FlinkEnvFactory.create(conf);

        // TODO(M4-3) Kafka source 读 dwd-mes-report -> DwdReportRecord，配事件时间 watermark。
        //   watermark 策略与 DWD 作业保持一致（60s 乱序容忍 + idleness）。

        // TODO(M4-4) 分钟级产量聚合。
        //
        //   基础实现：
        //     keyBy(company + workshop + linecode + partcode)
        //       .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        //       .allowedLateness(Time.minutes(2))
        //       .sideOutputLateData(LATE_TAG)      // 迟到数据走 side output，计入 late_record_count
        //       .aggregate(...)
        //
        //   【数据倾斜】总装线既有手动报工又有扫码，单条线的量可能超过其他所有线之和（文档 11.1）。
        //   产量聚合是可交换可结合的，可以做两阶段聚合：
        //     一阶段 keyBy(维度 + "#" + rand(N)) 局部预聚合，N 取并行度的 2~4 倍
        //     二阶段 keyBy(维度) 去掉后缀二次聚合
        //   建议先不加盐跑一版，在 Flink Web UI 里看各 subtask 的 records received 是否明显不均，
        //   看到真实的倾斜之后再加——这样你讲的时候是"我观察到某条线是热点"，
        //   而不是"教程说要两阶段聚合"。
        //
        //   【迟到数据】side output 出来的不要直接丢，写到一个补数 topic 或直接 upsert 进 CK
        //   （ReplacingMergeTree 会覆盖），并且必须计数，否则就是静默丢数据。

        // TODO(M4-5) 订单级达成率。
        //     keyBy(company + shiftDate + productionorder)
        //       .process(new OrderAchievementFunction(conf.getLong("state.ttl.days", 7)))
        //       .addSink(new OrderAchievementCkSink(...))
        //
        //   计划进度需要订单的 createdDate / duedate，DWD 宽表里没有这两个字段。
        //   两个选择：
        //     a) 在 DWD 宽表里加上这两列（简单，但宽表变宽）
        //     b) 本作业也 bootstrap 一份订单维表（复用 mes-production-monitor-dwd 的 DimLoader）
        //   建议 a：这两个字段是订单的静态属性，放进明细层不会造成一致性问题。

        LOG.warn("DWS 作业骨架尚未完成，请先实现 M4-3 / M4-4 / M4-5 的 TODO");
        // env.execute("mes-production-monitor-dws-metrics");
    }
}
