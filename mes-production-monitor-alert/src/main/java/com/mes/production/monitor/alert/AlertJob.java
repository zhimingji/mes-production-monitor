package com.mes.production.monitor.alert;

import com.mes.production.monitor.alert.function.LineIdleDetectFunction;
import com.mes.production.monitor.alert.function.OrderAlertFunction;
import com.mes.production.monitor.alert.sink.AlertCkSink;
import com.mes.production.monitor.alert.sink.AlertKafkaSerializer;
import com.mes.production.monitor.alert.sink.DingTalkSink;
import com.mes.production.monitor.common.constant.Topics;
import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.AlertRecord;
import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.common.util.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * 作业 4：报工异常告警
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

        // ---------- M5-5 Kafka source：dwd-mes-report -> DwdReportRecord ----------
        // 源是 DWD 已消化好的普通 JSON（最新值宽表），墓碑（isDeleted=true）也要放行——
        // OrderAlertFunction 要靠它做报工回撤。事件时间取 outputDateTime，乱序容忍 + 空闲分区
        // 都从配置读。与 DWS 不同的是：这里 watermark 配在共享的 dwdStream 上，
        // 因为 OrderAlertFunction 是事件时间驱动，漏报那路只注册处理时间 Timer、不受影响。
        KafkaSource<String> dwdKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setTopics(conf.getString("kafka.dwd.topic", Topics.DWD_MES_REPORT))
                .setGroupId(conf.getString("kafka.group.id", "mes-production-monitor-alert"))
                .setStartingOffsets(dwdStartupOffset(conf))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        long outOfOrdernessSec = conf.getLong("watermark.out.of.orderness.seconds", 60L);
        long idlenessSec = conf.getLong("watermark.idleness.seconds", 60L);

        DataStream<DwdReportRecord> dwdStream = env
                .fromSource(dwdKafkaSource, WatermarkStrategy.noWatermarks(), "dwd-kafka-source")
                .uid("dwd-kafka-source")
                .flatMap(new DwdReportParser())
                .name("dwd-report-parse")
                .uid("dwd-report-parse")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DwdReportRecord>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSec))
                                .withTimestampAssigner((r, ts) -> r.outputDateTime)
                                .withIdleness(Duration.ofSeconds(idlenessSec)))
                .name("dwd-report-watermark")
                .uid("dwd-report-watermark");

        // ---------- M5-6 两路检测 + union ----------
        DataStream<AlertRecord> orderAlerts = dwdStream
                .keyBy(r -> nvl(r.company) + "|" + nvl(r.dataSource) + "|" + r.shiftDate + "|" + nvl(r.productionOrder))
                .process(new OrderAlertFunction(
                        conf.getDouble("alert.over.report.ratio", 1.0),
                        conf.getDouble("alert.lag.threshold", 0.2),
                        conf.getLong("alert.escalate.interval.ms", 600_000L),
                        conf.getLong("state.ttl.days", 7L)))
                .name("order-alert").uid("order-alert");

        DataStream<AlertRecord> idleAlerts = dwdStream
                .keyBy(r -> nvl(r.company) + "|" + nvl(r.lineCode))
                .process(new LineIdleDetectFunction(
                        conf.getLong("alert.idle.threshold.ms", 3_600_000L),
                        conf.getLong("alert.escalate.interval.ms", 600_000L)))
                .name("line-idle-detect").uid("line-idle-detect");

        DataStream<AlertRecord> alerts = orderAlerts.union(idleAlerts);

        // ---------- M5-7 三路 sink：Kafka + ClickHouse + 钉钉 ----------
        // 1. Kafka mes-alert：分区键 company:linecode:productionorder（文档 5.1）。
        //    2PC 事务超时须 > checkpoint 周期(60s)、<= broker transaction.max.timeout.ms(默认 15min)，见文档 12。
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("transaction.timeout.ms", "300000");

        KafkaSink<AlertRecord> kafkaAlertSink = KafkaSink.<AlertRecord>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setRecordSerializer(new AlertKafkaSerializer(
                        conf.getString("kafka.alert.topic", Topics.MES_ALERT)))
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("mes-alert-")
                .setKafkaProducerConfig(kafkaProducerProps)
                .build();
        alerts.sinkTo(kafkaAlertSink).name("kafka-alert-sink").uid("kafka-alert-sink");

        // 2. ClickHouse ads_alert：攒批阈值比明细小，否则告警会被压在缓冲区里出不来（见配置注释）。
        alerts.addSink(new AlertCkSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 100),
                        conf.getLong("clickhouse.flush.interval.ms", 2000L)))
                .name("ck-alert-sink").uid("ck-alert-sink");

        // 3. 钉钉 Webhook：一定放在最后，且不能影响主链路——
        //    推送失败只记日志、不抛异常（AlertRecord 已落 Kafka 和 CK，通知是尽力而为）。
        //    限流是 sink 本地状态（Deque），必须 setParallelism(1) 才全局生效。
        alerts.addSink(new DingTalkSink(
                        conf.getString("alert.dingtalk.webhook", ""),
                        conf.getInt("alert.dingtalk.max.per.minute", 18),
                        conf.getBoolean("alert.dingtalk.enabled", false),
                        conf.getLong("alert.dingtalk.flush.interval.ms", 5000L)))
                .name("dingtalk-sink").uid("dingtalk-sink").setParallelism(1);

        // 验收方式（见文档 9.3.1）：用造数器定向注入异常，逐个确认能被捕获——
        //   --anomaly.over-report 某订单报工量超过订单量  -> 应触发 OVER_REPORT
        //   --anomaly.line-idle   指定产线停写 N 分钟     -> 应触发 MISSING_REPORT
        //   --anomaly.lag         订单进度落后爬坡曲线    -> 应触发 LAG + 分级升级
        // 如果造出来的数据全是正常生产，告警永远不触发，正确性无从验证。

        env.execute("mes-production-monitor-alert");
    }

    private static OffsetsInitializer dwdStartupOffset(JobConfig conf) {
        switch (conf.getString("kafka.dwd.startup", "earliest")) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            default:
                throw new IllegalArgumentException("未知的 kafka.dwd.startup（支持 earliest / latest）");
        }
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    /**
     * 解析 {@code dwd-mes-report} 的 JSON -> {@link DwdReportRecord}。
     *
     * <p>墓碑（{@code isDeleted=true}）不丢弃、只计数——它必须流到下游做回撤：
     * 超报/滞后的累计报工量要据此减掉被删的报工（见 {@link OrderAlertFunction}）。
     * 静默丢弃是数据链路里最危险的事。
     */
    public static class DwdReportParser extends RichFlatMapFunction<String, DwdReportRecord> {
        private static final long serialVersionUID = 1L;

        private transient Counter dirtyCount;
        private transient Counter deletedCount;

        @Override
        public void open(Configuration parameters) {
            MetricGroup g = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
            this.dirtyCount = g.counter(MetricNames.DIRTY_RECORD_COUNT);
            this.deletedCount = g.counter(MetricNames.DELETED_RECORD_COUNT);
        }

        @Override
        public void flatMap(String json, Collector<DwdReportRecord> out) {
            DwdReportRecord r;
            try {
                r = JsonUtil.fromJson(json, DwdReportRecord.class);
            } catch (Exception e) {
                dirtyCount.inc();
                LOG.warn("DWD 记录解析失败，已丢弃: {}", json, e);
                return;
            }
            if (r == null) {
                dirtyCount.inc();
                return;
            }
            if (r.isDeleted) {
                deletedCount.inc();
            }
            out.collect(r);
        }
    }
}
