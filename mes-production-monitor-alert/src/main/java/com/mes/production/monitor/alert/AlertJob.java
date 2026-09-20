package com.mes.production.monitor.alert;

import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业 4：报工异常告警。见文档 6.4。
 *
 * <pre>
 * Kafka dwd-mes-report
 *        │
 *        ├── keyBy(订单)  OrderAlertFunction     ── 超报 OVER_REPORT / 滞后 LAG（事件时间驱动）
 *        └── keyBy(产线)  LineIdleDetectFunction ── 漏报 MISSING_REPORT（处理时间 Timer！）
 *                │
 *                union
 *                ├──> Kafka mes-alert
 *                ├──> ClickHouse ads_alert
 *                └──> 钉钉 Webhook（带限流）
 * </pre>
 *
 * <p>两个检测算子的<b>时间语义不同</b>，这不是随意选择：超报和滞后由新数据驱动，用事件时间；
 * 漏报的触发条件恰恰是"没有数据"，事件时间下 watermark 不推进、Timer 永不触发，
 * 必须用处理时间。见文档 6.4.2。
 */
public class AlertJob {

    private static final Logger LOG = LoggerFactory.getLogger(AlertJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("alert.properties", args);
        StreamExecutionEnvironment env = FlinkEnvFactory.create(conf);

        // TODO(M5-5) Kafka source 读 dwd-mes-report -> DwdReportRecord。

        // TODO(M5-6) 两路检测 + union
        //
        // DataStream<AlertRecord> orderAlerts = dwdStream
        //         .keyBy(r -> r.company + "|" + r.shiftDate + "|" + r.productionOrder)
        //         .process(new OrderAlertFunction(
        //                 conf.getDouble("alert.over.report.ratio", 1.0),
        //                 conf.getDouble("alert.lag.threshold", 0.2),
        //                 conf.getLong("alert.escalate.interval.ms", 600_000L),
        //                 conf.getLong("state.ttl.days", 7)))
        //         .name("order-alert").uid("order-alert");
        //
        // DataStream<AlertRecord> idleAlerts = dwdStream
        //         .keyBy(r -> r.company + "|" + r.lineCode)
        //         .process(new LineIdleDetectFunction(
        //                 conf.getLong("alert.idle.threshold.ms", 3_600_000L),
        //                 conf.getLong("alert.escalate.interval.ms", 600_000L)))
        //         .name("line-idle-detect").uid("line-idle-detect");
        //
        // DataStream<AlertRecord> alerts = orderAlerts.union(idleAlerts);

        // TODO(M5-7) 三路 sink：Kafka + ClickHouse + 钉钉
        //   钉钉那路一定要放在最后且不能影响主链路：
        //   推送失败只记日志，不抛异常（AlertRecord 已经落了 Kafka 和 CK，通知是尽力而为）。
        //
        // alerts.sinkTo(kafkaAlertSink).name("kafka-alert-sink").uid("kafka-alert-sink");
        // alerts.addSink(new AlertCkSink(...)).name("ck-alert-sink").uid("ck-alert-sink");
        // alerts.addSink(new DingTalkSink(
        //         conf.getString("alert.dingtalk.webhook", ""),
        //         conf.getInt("alert.dingtalk.max.per.minute", 18),
        //         conf.getBoolean("alert.dingtalk.enabled", false)))
        //         .name("dingtalk-sink").uid("dingtalk-sink").setParallelism(1);

        // 验收方式（见文档 9.3.1）：用造数器定向注入异常，逐个确认能被捕获——
        //   --anomaly.over-report 某订单报工量超过订单量  -> 应触发 OVER_REPORT
        //   --anomaly.line-idle   指定产线停写 N 分钟     -> 应触发 MISSING_REPORT
        //   --anomaly.lag         订单进度落后爬坡曲线    -> 应触发 LAG + 分级升级
        // 如果造出来的数据全是正常生产，告警永远不触发，正确性无从验证。

        LOG.warn("告警作业骨架尚未完成，请先实现 M5-5 / M5-6 / M5-7 的 TODO");
        // env.execute("mes-production-monitor-alert");
    }
}
