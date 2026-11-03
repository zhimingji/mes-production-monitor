package com.mes.production.monitor.dws;

import com.mes.production.monitor.common.constant.Topics;
import com.mes.production.monitor.common.flink.FlinkEnvFactory;
import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.MinuteDelta;
import com.mes.production.monitor.common.model.MinuteMetric;
import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.common.util.JsonUtil;
import com.mes.production.monitor.dws.function.ChangelogNormalizer;
import com.mes.production.monitor.dws.function.OrderAchievementFunction;
import com.mes.production.monitor.dws.sink.MinuteMetricCkSink;
import com.mes.production.monitor.dws.sink.OrderAchievementCkSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 作业 3：DWD -&gt; DWS 指标聚合。见文档 6.3。
 *
 * <pre>
 * Kafka dwd-mes-report
 *        │
 *        ├── changelog 归一化 ──> 1min 窗口累加 ──> SummingMergeTree dws_report_1min
 *        └── keyBy(订单) 累计达成率 ──────────────> ClickHouse dws_order_achievement
 * </pre>
 *
 * <p><b>两个粒度放在同一个作业里</b>，用多路 sink 输出，不要拆成两个作业——
 * 两个作业会重复消费同一 topic、重复解析，浪费资源且口径容易漂移。
 *
 * <p><b>分钟链路为什么先归一化再累加</b>：DWD 是明细直通流，同一 record 修正/删除会再来一条，
 * 直接 SUM 会重复计数。这里先 {@link ChangelogNormalizer} 把每条记录翻译成带符号增量，
 * 再交给 SummingMergeTree 累加——迟到数据只是一条普通 INSERT，CK merge 时自动求和（见文档 6.3）。
 */
public class DwsMetricsJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwsMetricsJob.class);

    /** 迟到增量侧流标签：超出 allowedLateness 的增量从这旁路出去，直接 INSERT 进 SummingMergeTree 补数 */
    private static final OutputTag<MinuteDelta> LATE_TAG =
            new OutputTag<MinuteDelta>("late-minute-delta") {};

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("dws.properties", args);
        StreamExecutionEnvironment env = FlinkEnvFactory.create(conf);

        // ---------- M4-3 Kafka source：dwd-mes-report -> DwdReportRecord ----------
        // 源是普通 JSON（DWD 已把 changelog 消化成最新值），墓碑（isDeleted=true）也要放行，
        // 供下游回撤。事件时间在 changelog 归一化之后、分钟链路上单独配 watermark。
        KafkaSource<String> dwdKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(conf.getString("kafka.bootstrap.servers"))
                .setTopics(conf.getString("kafka.dwd.topic", Topics.DWD_MES_REPORT))
                .setGroupId(conf.getString("kafka.group.id", "mes-production-monitor-dws"))
                .setStartingOffsets(dwdStartupOffset(conf))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        long outOfOrdernessSec = conf.getLong("watermark.out.of.orderness.seconds", 60L);
        long idlenessSec = conf.getLong("watermark.idleness.seconds", 60L);
        long allowedLatenessMin = conf.getLong("window.allowed.lateness.minutes", 2L);

        DataStream<DwdReportRecord> rawStream = env
                .fromSource(dwdKafkaSource, WatermarkStrategy.noWatermarks(), "dwd-kafka-source")
                .uid("dwd-kafka-source")
                .flatMap(new DwdReportParser())
                .name("dwd-report-parse")
                .uid("dwd-report-parse");

        // ---------- 分钟级产量：changelog 归一化 -> 带符号增量 -> SummingMergeTree ----------
        DataStream<MinuteDelta> deltaStream = rawStream
                .keyBy(DwdReportRecord::logicalKey)
                .process(new ChangelogNormalizer())
                .name("changelog-normalize")
                .uid("changelog-normalize")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MinuteDelta>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSec))
                                .withTimestampAssigner((d, ts) -> d.eventTime)
                                .withIdleness(Duration.ofSeconds(idlenessSec)))
                .name("minute-delta-watermark")
                .uid("minute-delta-watermark");

        long windowMinutes = conf.getLong("metric.window.minutes", 1L);
        int saltBuckets = conf.getInt("agg.salt.buckets", 1);

        SingleOutputStreamOperator<MinuteMetric> minuteStream;
        DataStream<MinuteDelta> lateDeltaStream;
        if (saltBuckets > 1) {
            // 两阶段聚合（数据倾斜，文档 11.1）：一阶段 key 加随机盐预聚合，二阶段去盐二次聚合。
            // 盐随机即可——ChangelogNormalizer 已按 version 去重，checkpoint 一致回滚保证不重不漏。
            SingleOutputStreamOperator<MinuteMetric> preAgg = deltaStream
                    .keyBy(d -> saltedMinuteKey(d, saltBuckets))
                    .window(TumblingEventTimeWindows.of(Time.minutes(windowMinutes)))
                    .allowedLateness(Time.minutes(allowedLatenessMin))
                    .sideOutputLateData(LATE_TAG)
                    .aggregate(new MinuteDeltaAggregate(), new MinuteMetricWindow())
                    .name("minute-pre-aggregate")
                    .uid("minute-pre-aggregate");

            lateDeltaStream = preAgg.getSideOutput(LATE_TAG);

            DataStream<MinuteMetric> preAggTs = preAgg
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<MinuteMetric>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSec))
                                    .withTimestampAssigner((m, ts) -> m.tsMinute)
                                    .withIdleness(Duration.ofSeconds(idlenessSec)))
                    .name("minute-pre-watermark")
                    .uid("minute-pre-watermark");

            minuteStream = preAggTs
                    .keyBy(DwsMetricsJob::minuteMetricKey)
                    .window(TumblingEventTimeWindows.of(Time.minutes(windowMinutes)))
                    .aggregate(new MinuteMetricMerge(), new MinuteMetricWindow())
                    .name("minute-final-aggregate")
                    .uid("minute-final-aggregate");
        } else {
            minuteStream = deltaStream
                    .keyBy(DwsMetricsJob::minuteDeltaKey)
                    .window(TumblingEventTimeWindows.of(Time.minutes(windowMinutes)))
                    .allowedLateness(Time.minutes(allowedLatenessMin))
                    .sideOutputLateData(LATE_TAG)
                    .aggregate(new MinuteDeltaAggregate(), new MinuteMetricWindow())
                    .name("minute-aggregate")
                    .uid("minute-aggregate");
            lateDeltaStream = minuteStream.getSideOutput(LATE_TAG);
        }

        // 迟到增量：直接 INSERT 进 SummingMergeTree（带原始 ts_minute），CK merge 时自动补进对应分钟
        lateDeltaStream
                .map(DwsMetricsJob::toMinuteMetric)
                .name("late-delta-to-metric")
                .uid("late-delta-to-metric")
                .addSink(new MinuteMetricCkSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 2000),
                        conf.getLong("clickhouse.flush.interval.ms", 5000L)))
                .name("ck-late-minute-sink")
                .uid("ck-late-minute-sink");

        minuteStream.addSink(new MinuteMetricCkSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 2000),
                        conf.getLong("clickhouse.flush.interval.ms", 5000L)))
                .name("ck-minute-metric-sink")
                .uid("ck-minute-metric-sink");

        // ---------- 订单级达成率 ----------
        //
        //   keyBy(company|datasource|shiftDate|productionOrder) -> 累计报工量 + 达成率 + 计划进度，
        //   定时输出快照到 CK（订单去重表），维度级达成率交给查询侧现算（见 7.2 / 6.3.2）。
        rawStream
                .keyBy(DwsMetricsJob::orderKey)
                .process(new OrderAchievementFunction(
                        conf.getLong("state.ttl.days", 7L),
                        conf.getLong("order.snapshot.emit.interval.ms", 10_000L)))
                .name("order-achievement")
                .uid("order-achievement")
                .addSink(new OrderAchievementCkSink(
                        conf.getString("clickhouse.jdbc.url"),
                        conf.getString("clickhouse.username"),
                        conf.getString("clickhouse.password"),
                        conf.getInt("clickhouse.batch.size", 2000),
                        conf.getLong("clickhouse.flush.interval.ms", 5000L)))
                .name("ck-order-achievement-sink")
                .uid("ck-order-achievement-sink");

        env.execute("mes-production-monitor-dws-metrics");
    }

    /** 分钟增量聚合键：company|workshop|linecode|partcode */
    private static String minuteDeltaKey(MinuteDelta d) {
        return nvl(d.company) + '|' + nvl(d.workshop) + '|' + nvl(d.lineCode) + '|' + nvl(d.partCode);
    }

    /** 一阶段加盐键：维度 + "#" + 随机盐（buckets 内随机；ChangelogNormalizer 已去重故无需确定性） */
    private static String saltedMinuteKey(MinuteDelta d, int buckets) {
        return minuteDeltaKey(d) + '#' + ThreadLocalRandom.current().nextInt(buckets);
    }

    /** 分钟聚合的去盐键（二阶段）：company|workshop|linecode|partcode */
    private static String minuteMetricKey(MinuteMetric m) {
        return nvl(m.company) + '|' + nvl(m.workshop) + '|' + nvl(m.lineCode) + '|' + nvl(m.partCode);
    }

    /** 订单达成率键：company|datasource|shiftDate|productionOrder。加 datasource 避免 old/new 库同名订单撞键翻倍 */
    private static String orderKey(DwdReportRecord r) {
        return nvl(r.company) + '|' + nvl(r.dataSource) + '|' + r.shiftDate + '|' + nvl(r.productionOrder);
    }

    /** 迟到增量转 MinuteMetric：按 eventTime 归到其所属分钟，交给 SummingMergeTree 累加 */
    private static MinuteMetric toMinuteMetric(MinuteDelta d) {
        MinuteMetric m = new MinuteMetric();
        m.tsMinute = d.eventTime - Math.floorMod(d.eventTime, 60_000L);
        m.company = d.company;
        m.workshop = d.workshop;
        m.lineCode = d.lineCode;
        m.lineName = d.lineName;
        m.partCode = d.partCode;
        m.outputQuantity = d.quantity;
        m.recordCnt = d.count;
        return m;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
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

    // ==================== M4-3 解析 ====================

    /**
     * 解析 {@code dwd-mes-report} 的 JSON -&gt; {@link DwdReportRecord}。
     *
     * <p>墓碑（{@code isDeleted=true}）不丢弃、只计数——它必须流到下游做回撤：
     * 分钟侧由 {@link ChangelogNormalizer} 转成负数增量，订单侧由 {@link OrderAchievementFunction}
     * 的 MapState 做减法。静默丢弃是数据链路里最危险的事。
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

    // ==================== 分钟聚合（带符号增量累加） ====================

    /** 累加器：SUM(quantity) + SUM(count)，维度在 key 内恒定取第一条 */
    public static class MinuteAccumulator {
        public BigDecimal sum = BigDecimal.ZERO;
        public long count = 0;
        public boolean initialized = false;
        public String company;
        public String workshop;
        public String lineCode;
        public String lineName;
        public String partCode;
    }

    public static class MinuteDeltaAggregate
            implements AggregateFunction<MinuteDelta, MinuteAccumulator, MinuteMetric> {
        private static final long serialVersionUID = 1L;

        @Override
        public MinuteAccumulator createAccumulator() {
            return new MinuteAccumulator();
        }

        @Override
        public MinuteAccumulator add(MinuteDelta d, MinuteAccumulator acc) {
            if (!acc.initialized) {
                acc.company = d.company;
                acc.workshop = d.workshop;
                acc.lineCode = d.lineCode;
                acc.lineName = d.lineName;
                acc.partCode = d.partCode;
                acc.initialized = true;
            }
            acc.sum = acc.sum.add(d.quantity == null ? BigDecimal.ZERO : d.quantity);
            acc.count += d.count;
            return acc;
        }

        @Override
        public MinuteAccumulator merge(MinuteAccumulator a, MinuteAccumulator b) {
            if (!a.initialized) {
                return b;
            }
            if (!b.initialized) {
                return a;
            }
            a.sum = a.sum.add(b.sum);
            a.count += b.count;
            return a;
        }

        @Override
        public MinuteMetric getResult(MinuteAccumulator acc) {
            MinuteMetric m = new MinuteMetric();
            m.company = acc.company;
            m.workshop = acc.workshop;
            m.lineCode = acc.lineCode;
            m.lineName = acc.lineName;
            m.partCode = acc.partCode;
            m.outputQuantity = acc.sum;
            m.recordCnt = acc.count;
            return m;
        }
    }

    /** 二阶段聚合：把一阶段的 N 个部分和按维度求和（复用 MinuteAccumulator） */
    public static class MinuteMetricMerge
            implements AggregateFunction<MinuteMetric, MinuteAccumulator, MinuteMetric> {
        private static final long serialVersionUID = 1L;

        @Override
        public MinuteAccumulator createAccumulator() {
            return new MinuteAccumulator();
        }

        @Override
        public MinuteAccumulator add(MinuteMetric m, MinuteAccumulator acc) {
            if (!acc.initialized) {
                acc.company = m.company;
                acc.workshop = m.workshop;
                acc.lineCode = m.lineCode;
                acc.lineName = m.lineName;
                acc.partCode = m.partCode;
                acc.initialized = true;
            }
            acc.sum = acc.sum.add(m.outputQuantity == null ? BigDecimal.ZERO : m.outputQuantity);
            acc.count += m.recordCnt;
            return acc;
        }

        @Override
        public MinuteAccumulator merge(MinuteAccumulator a, MinuteAccumulator b) {
            if (!a.initialized) {
                return b;
            }
            if (!b.initialized) {
                return a;
            }
            a.sum = a.sum.add(b.sum);
            a.count += b.count;
            return a;
        }

        @Override
        public MinuteMetric getResult(MinuteAccumulator acc) {
            MinuteMetric m = new MinuteMetric();
            m.company = acc.company;
            m.workshop = acc.workshop;
            m.lineCode = acc.lineCode;
            m.lineName = acc.lineName;
            m.partCode = acc.partCode;
            m.outputQuantity = acc.sum;
            m.recordCnt = acc.count;
            return m;
        }
    }

    /** 补齐窗口起始时间 ts_minute，其余字段由聚合结果带上 */
    public static class MinuteMetricWindow
            extends ProcessWindowFunction<MinuteMetric, MinuteMetric, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String key, Context ctx, Iterable<MinuteMetric> elements, Collector<MinuteMetric> out) {
            MinuteMetric m = elements.iterator().next();
            m.tsMinute = ctx.window().getStart();
            out.collect(m);
        }
    }
}
