package com.mes.production.monitor.alert.function;

import com.mes.production.monitor.common.model.AlertRecord;
import com.mes.production.monitor.common.model.DwdReportRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
    }

    @Override
    public void processElement(DwdReportRecord value,
                              Context ctx,
                              Collector<AlertRecord> out) throws Exception {
        // TODO(M5-1) 有报工到达时：
        //   1) 更新 lastReportTs = ctx.timerService().currentProcessingTime()
        //   2) 如果当前有告警（currentLevel != NONE），说明产线恢复了：
        //      输出一条 recoverTime 非空的 AlertRecord 关闭告警，currentLevel 置 NONE。
        //      注意 alertId 必须与首次触发时一致，否则 ClickHouse 里会多出一条记录
        //      而不是覆盖原记录。
        //   3) 重置续期 Timer：
        //      先 deleteProcessingTimeTimer(旧值)，再
        //      registerProcessingTimeTimer(now + idleThresholdMs)，
        //      并把新时间戳存进 registeredTimer。
        //
        //   【为什么要先删】不删的话每条报工都注册一个新 Timer，
        //   状态里会堆积大量 Timer，且旧 Timer 到点会误判漏报。
        throw new UnsupportedOperationException("TODO(M5-1): 见方法内注释");
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertRecord> out) throws Exception {
        // TODO(M5-2) Timer 触发 = 该产线静默超阈值：
        //   1) 再校验一次 lastReportTs：Timer 可能是过期的旧 Timer（虽然上面已删，仍要防御）。
        //      if (now - lastReportTs < idleThresholdMs) return;
        //   2) 级别升级：currentLevel = AlertLevel.valueOf(currentLevel).escalate()
        //   3) 输出 AlertRecord（alertType = MISSING_REPORT，alertId 保持稳定）
        //   4) 若还没到 P1，继续注册下一个升级 Timer：
        //      registerProcessingTimeTimer(now + escalateIntervalMs)
        //      到 P1 后停止升级，避免无限刷屏
        throw new UnsupportedOperationException("TODO(M5-2): 见方法内注释");
    }
}
