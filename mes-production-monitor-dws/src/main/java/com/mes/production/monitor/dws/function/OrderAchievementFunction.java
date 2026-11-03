package com.mes.production.monitor.dws.function;

import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.OrderAchievement;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 订单级达成率的一级聚合。{@code keyBy(company + shiftDate + productionOrder)}。
 *
 * <h3>为什么只做到订单粒度</h3>
 * 维度级达成率需要二级聚合，而一级聚合的输出是<b>持续更新的值</b>，
 * 在 Flink 里做二级聚合必须处理回撤流（DataStream 要自己维护"上次发出的值"发增量修正），
 * 复杂度和状态成本都高。
 *
 * <p>本项目把订单粒度结果落到 ClickHouse，维度聚合交给查询侧现算——
 * 落地表已经是订单去重的，维度 {@code SUM(report)/SUM(order_qty)} 不会重复累加 orderQuantity，
 * 口径依然正确。见文档 6.3.2 / 7.2。
 *
 * <h3>报工修正 / 物理删除的回撤（changelog 语义的代价）</h3>
 * DWD 是明细直通流，同一个 record_id 修正后会再来一条，直接累加会重复计入。
 * 这里用 {@code MapState<record 逻辑键, 上次报工量>} 做减法回撤：累加时先减旧值再加新值，
 * 删除墓碑则只减不加。状态从 O(1) 变成 O(订单内报工笔数)，热点订单要留意（见文档 11.1）。
 *
 * <h3>输出节流 + 计划进度</h3>
 * 每来一笔报工就输出会把 ClickHouse 写爆。这里用处理时间 Timer 定时输出快照（
 * {@code order.snapshot.emit.interval.ms}），计划进度随真实时间在每次输出时重算，见文档 7.3。
 *
 * <h3>状态 TTL 已配置</h3>
 * 订单键是<b>无界增长</b>的，不清理就是定时炸弹。这里配了兜底 TTL，
 * 业务层的主动清理（订单完工 / 过交期 / 班次切换）在 {@link #onTimer} 里实现，见文档 6.4.1。
 */
public class OrderAchievementFunction
        extends KeyedProcessFunction<String, DwdReportRecord, OrderAchievement> {

    private static final long serialVersionUID = 1L;

    private final long stateTtlDays;
    private final long emitIntervalMs;

    /** 订单累计状态。只存累计值（O(1)），不存明细列表——热点订单下这是唯一能扛住的设计（文档 11.1） */
    private transient ValueState<OrderAchievement> accState;
    /** record 逻辑键 -> 上次累计的报工量，用于报工修正/删除的回撤 */
    private transient MapState<String, BigDecimal> lastQtyByRecordId;
    /** 是否已注册节流输出 timer（避免每条报工重复注册一个 timer） */
    private transient ValueState<Boolean> emitScheduled;

    public OrderAchievementFunction(long stateTtlDays, long emitIntervalMs) {
        this.stateTtlDays = stateTtlDays;
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.days(stateTtlDays))
                // 每次写入刷新过期时间：活跃订单不会被误清
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 过期数据即使还没被物理清理也绝不返回，避免读到脏累计值
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // RocksDB 后台 compaction 时顺带清理，不需要额外扫描
                .cleanupInRocksdbCompactFilter(10_000L)
                .build();

        ValueStateDescriptor<OrderAchievement> desc =
                new ValueStateDescriptor<>("order-acc", OrderAchievement.class);
        desc.enableTimeToLive(ttl);
        this.accState = getRuntimeContext().getState(desc);

        MapStateDescriptor<String, BigDecimal> qtyDesc =
                new MapStateDescriptor<>("last-qty-by-record", String.class, BigDecimal.class);
        qtyDesc.enableTimeToLive(ttl);
        this.lastQtyByRecordId = getRuntimeContext().getMapState(qtyDesc);

        ValueStateDescriptor<Boolean> scheduledDesc =
                new ValueStateDescriptor<>("emit-scheduled", Boolean.class);
        this.emitScheduled = getRuntimeContext().getState(scheduledDesc);
    }

    @Override
    public void processElement(DwdReportRecord value,
                               Context ctx,
                               Collector<OrderAchievement> out) throws Exception {
        OrderAchievement acc = accState.value();
        if (acc == null) {
            acc = new OrderAchievement();
            acc.shiftDate = value.shiftDate;
            acc.company = value.company;
            acc.dataSource = value.dataSource;
            acc.productionOrder = value.productionOrder;
            acc.productionOrderId = value.productionOrderId;
            acc.workshop = value.workshop;
            acc.lineCode = value.lineCode;
            acc.partCode = value.partCode;
            acc.orderQuantity = value.orderQuantity;
            acc.orderCreatedDate = value.orderCreatedDate;
            acc.orderDueDate = value.orderDueDate;
            acc.reportQuantity = BigDecimal.ZERO;
        }

        // 报工修正 / 物理删除的回撤：先减该 record 上次累计的量，再按最新值累加。
        // 直接 SUM 会把修正前后的量重复计入，这是 changelog 语义在聚合层的真实代价。
        String recordKey = value.logicalKey();
        BigDecimal oldQty = lastQtyByRecordId.get(recordKey);
        BigDecimal newQty = value.isDeleted
                ? BigDecimal.ZERO
                : (value.lotQuantity == null ? BigDecimal.ZERO : value.lotQuantity);
        if (oldQty != null) {
            acc.reportQuantity = acc.reportQuantity.subtract(oldQty);
        }
        acc.reportQuantity = acc.reportQuantity.add(newQty);

        if (value.isDeleted) {
            lastQtyByRecordId.remove(recordKey);
        } else {
            lastQtyByRecordId.put(recordKey, newQty);
        }

        if (value.outputDateTime > acc.lastReportTime) {
            acc.lastReportTime = value.outputDateTime;
        }
        acc.version = ctx.timerService().currentProcessingTime();
        accState.update(acc);

        // 输出节流：只在没有 pending timer 时注册，避免每条报工注册一个 timer
        Boolean scheduled = emitScheduled.value();
        if (scheduled == null || !scheduled) {
            long fireAt = ctx.timerService().currentProcessingTime() + emitIntervalMs;
            ctx.timerService().registerProcessingTimeTimer(fireAt);
            emitScheduled.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderAchievement> out) throws Exception {
        OrderAchievement acc = accState.value();
        if (acc == null) {
            return;
        }
        long now = ctx.timerService().currentProcessingTime();

        // 达成率（订单量为 0 时跳过，别抛 ArithmeticException）
        if (acc.orderQuantity > 0) {
            acc.achieveRate = acc.reportQuantity
                    .divide(BigDecimal.valueOf(acc.orderQuantity), 4, RoundingMode.HALF_UP);
        } else {
            acc.achieveRate = BigDecimal.ZERO;
        }

        // 计划进度（线性爬坡）：已用时间 / 生产周期，截断到 [0, 1]
        acc.planProgress = computePlanProgress(acc, now);
        acc.version = now;

        out.collect(acc);

        // 完工：停止定时输出，但【不】清状态——迟到修正仍需按 record 回撤，状态靠 TTL 兜底清理。
        // 若这里 clear()，完工后再来一笔迟到修正会从 0 重算、丢历史累计量（changelog 语义的代价）。
        if (isFinished(acc, now)) {
            emitScheduled.clear();
            return;
        }

        // 未完：注册下一个输出 timer，让计划进度随真实时间持续推进
        ctx.timerService().registerProcessingTimeTimer(now + emitIntervalMs);
    }

    /** 计划进度 = 已用时间 / 生产周期。交期 <= 创建时间是脏数据（造数器刻意注入过），给 0 防除零 */
    private BigDecimal computePlanProgress(OrderAchievement acc, long now) {
        long created = acc.orderCreatedDate;
        long due = acc.orderDueDate;
        if (created <= 0 || due <= created) {
            return BigDecimal.ZERO;
        }
        BigDecimal elapsed = BigDecimal.valueOf(Math.max(0L, now - created));
        BigDecimal total = BigDecimal.valueOf(due - created);
        BigDecimal progress = elapsed.divide(total, 4, RoundingMode.HALF_UP);
        if (progress.signum() < 0) {
            return BigDecimal.ZERO;
        }
        if (progress.compareTo(BigDecimal.ONE) > 0) {
            return BigDecimal.ONE;
        }
        return progress;
    }

    private boolean isFinished(OrderAchievement acc, long now) {
        boolean achieved = acc.achieveRate != null && acc.achieveRate.compareTo(BigDecimal.ONE) >= 0;
        boolean pastDue = acc.orderDueDate > 0 && now >= acc.orderDueDate;
        return achieved && pastDue;
    }
}
