package com.mes.production.monitor.dwd;

import com.mes.production.monitor.common.constant.Topics;
import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.OdsReportRecord;
import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.dwd.dim.DimRefreshSource;
import com.mes.production.monitor.dwd.dim.DimUpdate;
import com.mes.production.monitor.dwd.dim.OrderCdcParser;
import com.mes.production.monitor.dwd.dim.ReportCdcParser;
import com.mes.production.monitor.dwd.function.DimBroadcastJoinFunction;
import com.mes.production.monitor.dwd.sink.DeletedReportAuditSink;
import com.mes.production.monitor.dwd.sink.DwdReportCkSink;
import com.mes.production.monitor.dwd.sink.DwdReportKafkaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * DWD 明细宽表
 *
 * <pre>
 * Kafka ods-mes-report (debezium-json)        Kafka ods-mes-order (debezium-json)
 *        │                                            │
 *        ├── 解析成 OdsReportRecord（两通道统一 POJO）   ├── 解析成 DimUpdate（订单 CDC 增量）
 *        │                                            │
 *        ├── connect(维表广播流) ── DimBroadcastJoinFunction
 *        │      订单关联 + 维表关联 + 口径过滤（本层不聚合）
 *        │
 *        ├──> Kafka dwd-mes-report   （供 DWS / 告警作业消费）
 *        └──> ClickHouse dwd_mes_report（明细查询 + 对账）
 *
 * 维表广播流 = 静态维表定时全量（DimRefreshSource）∪ 订单维 CDC 增量（OrderCdcParser），
 * 两者 union 成一个 DataStream&lt;DimUpdate&gt; 再广播，见文档 6.2.4。
 * </pre>
 */
public class DwdWideTableJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwdWideTableJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("dwd.properties", args);
        StreamExecutionEnvironment env = FlinkEnvFactory.create(conf);

        // ---------- 维表广播流 ----------
        // 并行度必须是 1：多个子任务同时全量查库没意义，广播下去还会互相覆盖
        // 现在广播流承载两类更新（union 成一个 DataStream<DimUpdate>）：
        //   1. 静态维表（workcenter/worksite/user）定时全量快照；part 走 PartLookup 按需查询，不广播；
        //   2. 订单维（productionorder）CDC 增量——它会 UPDATE，必须走 CDC，见文档 6.2.4。
        DataStream<DimUpdate> staticDimStream = env
                .addSource(new DimRefreshSource(
                        conf.getString("mysql.jdbc.url.template"),
                        conf.getString("mysql.username"),
                        conf.getString("mysql.password"),
                        conf.getLong("dim.refresh.interval.ms", 300_000L)))
                .name("dim-refresh-source")
                .setParallelism(1);

        KafkaSource<String> orderKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setTopics(conf.getString("kafka.ods.order.topic", Topics.ODS_MES_ORDER))
                .setGroupId(conf.getString("kafka.group.id", "mes-production-monitor-dwd"))
                .setStartingOffsets(orderStartupOffset(conf))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 订单维变更量小，单并行度即可；startup=earliest 配合幂等 upsert 安全回放历史，
        // checkpoint 会推进消费 offset（只有首次没有状态时才回退到 earliest）。
        DataStream<DimUpdate> orderCdcStream = env
                .fromSource(orderKafkaSource, WatermarkStrategy.noWatermarks(), "order-cdc-source")
                .uid("order-cdc-source")
                .setParallelism(1)
                .flatMap(new OrderCdcParser())
                .uid("order-cdc-parse")
                .setParallelism(1);

        BroadcastStream<DimUpdate> dimBroadcast = staticDimStream
                .union(orderCdcStream)
                .broadcast(DimBroadcastJoinFunction.DIM_STATE_DESCRIPTOR);

        // ---------- 报工主数据流 ----------
        // 源是 debezium-json 字符串，事件时间要从解析后的 OdsReportRecord 里取，
        // 所以 source 先用 noWatermarks，flatMap 解析完再 assignTimestampsAndWatermarks。
        KafkaSource<String> reportKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setTopics(conf.getString("kafka.ods.topic", Topics.ODS_MES_REPORT))
                .setGroupId(conf.getString("kafka.group.id", "mes-production-monitor-dwd"))
                .setStartingOffsets(reportStartupOffset(conf))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        long outOfOrdernessSec = conf.getLong("watermark.out.of.orderness.seconds", 60L);
        long idlenessSec = conf.getLong("watermark.idleness.seconds", 60L);

        DataStream<OdsReportRecord> odsStream = env
                .fromSource(reportKafkaSource, WatermarkStrategy.noWatermarks(), "report-kafka-source")
                .uid("report-kafka-source")
                .flatMap(new ReportCdcParser())
                .name("report-cdc-parse")
                .uid("report-cdc-parse")
                // 事件时间 watermark：乱序容忍 + 空闲分区不阻塞全局水位
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OdsReportRecord>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSec))
                                .withTimestampAssigner((r, ts) -> r.eventTime())
                                .withIdleness(Duration.ofSeconds(idlenessSec)))
                .name("report-watermark")
                .uid("report-watermark");

        // ---------- connect 广播流做维表 join ----------
        // 类型用 SingleOutputStreamOperator（而非 DataStream）：后面要从它取侧流 getSideOutput
        SingleOutputStreamOperator<DwdReportRecord> dwdStream = odsStream
                .connect(dimBroadcast)
                .process(new DimBroadcastJoinFunction(
                        conf.getString("mysql.jdbc.url.template"),
                        conf.getString("mysql.username"),
                        conf.getString("mysql.password"),
                        conf.getInt("part.cache.max.size", 50000)))
                .name("dim-broadcast-join")
                .uid("dim-broadcast-join");

        // ---------- 双 sink ----------
        // 1. Kafka dwd-mes-report：分区键用 productionorder（订单编号），DWS 按订单聚合时局部化。
        //    2PC 事务超时须 > checkpoint 周期(60s)、<= broker transaction.max.timeout.ms(默认 15min)，见文档 12
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("transaction.timeout.ms", "300000");

        KafkaSink<DwdReportRecord> kafkaSink = KafkaSink.<DwdReportRecord>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setRecordSerializer(new DwdReportKafkaSerializer(
                        conf.getString("kafka.dwd.topic", Topics.DWD_MES_REPORT)))
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("mes-dwd-report-")
                .setKafkaProducerConfig(kafkaProducerProps)
                .build();
        dwdStream.sinkTo(kafkaSink).name("kafka-dwd-sink").uid("kafka-dwd-sink");

        // 2. ClickHouse：攒批参数从配置读，不写死
        dwdStream.addSink(new DwdReportCkSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 5000),
                        conf.getLong("clickhouse.flush.interval.ms", 10_000L)))
                .name("ck-dwd-sink")
                .uid("ck-dwd-sink");

        // 3. 物理删除审计：op=d 的完整 before 从侧流旁路落审计表，不丢不静默（见文档 6.1.1）
        DataStream<OdsReportRecord> deletedAuditStream = dwdStream
                .getSideOutput(DimBroadcastJoinFunction.DELETED_AUDIT_TAG);
        deletedAuditStream.addSink(new DeletedReportAuditSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 5000),
                        conf.getLong("clickhouse.flush.interval.ms", 10_000L)))
                .name("ck-deleted-audit-sink")
                .uid("ck-deleted-audit-sink");

        env.execute("mes-production-monitor-dwd-wide-table");
    }

    private static OffsetsInitializer orderStartupOffset(JobConfig conf) {
        switch (conf.getString("kafka.ods.order.startup", "earliest")) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            default:
                throw new IllegalArgumentException(
                        "未知的 kafka.ods.order.startup（支持 earliest / latest）");
        }
    }

    private static OffsetsInitializer reportStartupOffset(JobConfig conf) {
        switch (conf.getString("kafka.ods.report.startup", "earliest")) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            default:
                throw new IllegalArgumentException(
                        "未知的 kafka.ods.report.startup（支持 earliest / latest）");
        }
    }
}
