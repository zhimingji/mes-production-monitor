package com.mes.production.monitor.alert.function;

import com.mes.production.monitor.common.enums.AlertLevel;
import com.mes.production.monitor.common.enums.AlertType;
import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.AlertRecord;
import com.mes.production.monitor.common.model.DwdReportRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 漏报检测：产线连续 N 分钟无报工。{@code keyBy(company + linecode)}。
 *
 * <h2>核心陷阱：不能用事件时间 Timer（文档 6.4.2）</h2>
 *
 * <pre>
 * 产线停止报工 → 该分区没有新数据 → watermark 不推进
 *             → 事件时间不前进 → 事件时间 Timer 永不触发
 *             → 恰恰在最需要告警的时候，告警永远发不出来
 * </pre>
 *
 * 事件时间是<b>由数据驱动</b>的，没有数据就没有时间。而"多久没来数据"这个问题本质上属于
 * 物理世界的处理时间语义，所以必须用 {@code registerProcessingTimeTimer}。
 *
 * <p>注意 {@code WatermarkStrategy.withIdleness} 解决不了这个问题：它只能让空闲分区不拖累
 * 其他分区的 watermark 推进，如果全部分区都没数据，watermark 依然不动。
 *
 * <h3>强烈建议：故意踩一次这个坑</h3>
 * 先用 {@code registerEventTimeTimer} 实现，用造数器把某条产线断流一小时，
 * 观察告警确实没出来，再改成处理时间。亲手看到这个现象一次，比读十篇文章记得牢，
 * 而且这是面试区分度极高的一道题。
 */
public class LineIdleDetectFunction
        extends KeyedProcessFunction<String, DwdReportRecord, AlertRecord> {

    private static final long serialVersionUID = 1L;

    /** 判定漏报的静默时长（毫秒） */
    private final long idleThresholdMs;
    /** 未恢复时的升级间隔（毫秒）：P3 -> P2 -> P1 */
    private final long escalateIntervalMs;

    /** 该产线最后一次报工的处理时间 */
    private transient ValueState<Long> lastReportTs;
    /** 当前已注册的 Timer 时间戳。升级时必须先删旧 Timer 再注册新的，否则旧 Timer 到点会重复触发 */
    private transient ValueState<Long> registeredTimer;
    /** 当前告警级别（AlertLevel 的 name()） */
    private transient ValueState<String> currentLevel;
    /** 产线身份（onTimer 只有 key、没有 DwdReportRecord，须从状态取回） */
    private transient ValueState<String> company;
    private transient ValueState<String> lineCode;
    /** 首次触发时间（升级/恢复时保持 alert_time 不变，避免 CK 覆盖成升级时间） */
    private transient ValueState<Long> firstAlertTime;
    private transient MetricGroup metricGroup;

    public LineIdleDetectFunction(long idleThresholdMs, long escalateIntervalMs) {
        this.idleThresholdMs = idleThresholdMs;
        this.escalateIntervalMs = escalateIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.lastReportTs = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("last-report-ts", Long.class));
        this.registeredTimer = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("registered-timer", Long.class));
        this.currentLevel = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("current-level", String.class));
        this.company = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("company", String.class));
        this.lineCode = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("linecode", String.class));
        this.firstAlertTime = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("first-alert-time", Long.class));
        this.metricGroup = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
    }

    @Override
    public void processElement(DwdReportRecord value,
                              Context ctx,
                              Collector<AlertRecord> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();

        // 1) 更新最后报工时间 + 记录产线身份（供 onTimer 取回）
        lastReportTs.update(now);
        company.update(nvl(value.company));
        lineCode.update(nvl(value.lineCode));

        // 2) 有报工到达：若当前有告警，说明产线恢复，输出 recoverTime 关闭告警。
        //    alertId 与首次触发时一致，否则 ClickHouse 里会多出一条记录而不是覆盖。
        String current = currentLevel.value();
        if (current != null && !AlertLevel.NONE.name().equals(current)) {
            String c = nvl(value.company);
            String l = nvl(value.lineCode);
            long alertTime = firstAlertTime.value() == null ? now : firstAlertTime.value();
            out.collect(build(AlertLevel.NONE.name(), c, l, alertTime, now, now));
            currentLevel.update(AlertLevel.NONE.name());
            firstAlertTime.clear();
        }

        // 3) 重置续期 Timer：先删旧再注册新，否则每条报工堆积一个 Timer、旧 Timer 到点误判漏报。
        Long old = registeredTimer.value();
        if (old != null) {
            ctx.timerService().deleteProcessingTimeTimer(old);
        }
        long fireAt = now + idleThresholdMs;
        registeredTimer.update(fireAt);
        ctx.timerService().registerProcessingTimeTimer(fireAt);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertRecord> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();

        // 1) 再校验一次 lastReportTs：Timer 可能是过期的旧 Timer（虽然上面已删，仍要防御）。
        Long last = lastReportTs.value();
        if (last == null || now - last < idleThresholdMs) {
            return;
        }

        String c = nvl(company.value());
        String l = nvl(lineCode.value());

        // 2) 级别升级：NONE -> P3 -> P2 -> P1（P1 为终态）
        String current = currentLevel.value();
        AlertLevel next = AlertLevel.valueOf(current == null ? AlertLevel.NONE.name() : current).escalate();
        currentLevel.update(next.name());

        // 首次触发时间：从 NONE 升到 P3 时记录，后续升级保持不变
        long alertTime = now;
        if (firstAlertTime.value() != null) {
            alertTime = firstAlertTime.value();
        } else {
            firstAlertTime.update(now);
        }

        // 3) 输出 AlertRecord（alertType = MISSING_REPORT，alertId 保持稳定）
        out.collect(build(next.name(), c, l, alertTime, 0L, now));
        fireAlertMetric(AlertType.MISSING_REPORT.name(), next.name());

        // 4) 若还没到 P1，继续注册下一个升级 Timer；到 P1 停止升级，避免无限刷屏
        if (next != AlertLevel.P1) {
            long fireAt = now + escalateIntervalMs;
            registeredTimer.update(fireAt);
            ctx.timerService().registerProcessingTimeTimer(fireAt);
        } else {
            registeredTimer.clear();
        }
    }

    private AlertRecord build(String level, String c, String l,
                              long alertTime, long recoverTime, long version) {
        AlertRecord a = new AlertRecord();
        a.alertId = AlertType.MISSING_REPORT.name() + ":" + c + ":" + l;
        a.alertType = AlertType.MISSING_REPORT.name();
        a.level = level;
        a.company = c;
        a.lineCode = l;
        a.productionOrder = null; // 漏报是产线级，无订单
        a.orderQuantity = 0;
        a.reportQuantity = null;
        a.achieveRate = null;
        a.alertTime = alertTime;
        a.recoverTime = recoverTime;
        a.version = version;
        return a;
    }

    private void fireAlertMetric(String type, String level) {
        metricGroup.addGroup(type).addGroup(level).counter(MetricNames.ALERT_FIRED_COUNT).inc();
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
