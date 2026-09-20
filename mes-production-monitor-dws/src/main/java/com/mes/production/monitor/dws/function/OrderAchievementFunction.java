package com.mes.production.monitor.dws.function;

import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.OrderAchievement;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
 * <h3>状态 TTL 已配置</h3>
 * 订单键是<b>无界增长</b>的，不清理就是定时炸弹。这里配了兜底 TTL，
 * 业务层的主动清理（订单完工 / 过交期 / 班次切换）需要你在 processElement 里实现。见文档 6.4.1。
 */
public class OrderAchievementFunction
        extends KeyedProcessFunction<String, DwdReportRecord, OrderAchievement> {

    private static final long serialVersionUID = 1L;

    private final long stateTtlDays;

    /** 订单累计状态。只存累计值（O(1)），不存明细列表——热点订单下这是唯一能扛住的设计（文档 11.1） */
    private transient ValueState<OrderAchievement> accState;

    public OrderAchievementFunction(long stateTtlDays) {
        this.stateTtlDays = stateTtlDays;
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
    }

    @Override
    public void processElement(DwdReportRecord value,
                              Context ctx,
                              Collector<OrderAchievement> out) throws Exception {
        // TODO(M4-1) 实现订单累计 + 达成率 + 计划进度。
        //
        // 1) 累计：
        //      acc = accState.value()，为 null 则初始化（维度字段从 value 上拷一份）
        //      acc.reportQuantity = acc.reportQuantity + value.lotQuantity
        //      acc.lastReportTime = max(acc.lastReportTime, value.outputDateTime)
        //      acc.version = ctx.timerService().currentProcessingTime()  // 用于 CK 幂等覆盖
        //
        //    【重点】报工修正（同一 record_id 的 UPDATE）在这里是个真问题：
        //    DWD 层是明细直通的，同一个 lotid 修正后会再来一条，直接累加会重复计入。
        //    两种解法，想清楚再选：
        //      a) 本算子额外维护 MapState<recordId, 上次报工量>，累加时减去旧值
        //         —— 正确，但状态从 O(1) 变成 O(订单内报工笔数)，热点订单要小心；
        //      b) 消费 DWD topic 时就按 record_id 去重取最新
        //         —— 需要另一层有状态处理，本质是把问题挪了位置。
        //    这是"changelog 语义"在聚合层的真实代价，也是面试可以深聊的一段。
        //
        // 2) 达成率：acc.achieveRate = reportQuantity / orderQuantity
        //    注意 orderQuantity 为 0 的订单要跳过，别抛 ArithmeticException。
        //
        // 3) 计划进度（线性爬坡，文档 7.3）：
        //      生产周期 = order.dueDate - order.createdDate
        //      已用时间 = now - order.createdDate
        //      planProgress = 已用时间 / 生产周期，截断到 [0, 1]
        //    注意 dueDate <= createdDate 的脏数据要防（造数器里刻意注入过）。
        //
        // 4) 状态主动清理（TTL 只是兜底，文档 6.4.1）：
        //      达成率 >= 100% 且超过交期一定时间 -> accState.clear()
        //      或者用 Timer 在班次日切换后清理
        //
        // 5) 输出节流：每来一笔报工就 out.collect() 会把 ClickHouse 写爆。
        //    建议注册处理时间 Timer，每 N 秒输出该订单的当前快照。
        throw new UnsupportedOperationException("TODO(M4-1): 见方法内注释");
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderAchievement> out) throws Exception {
        // TODO(M4-2) 定时输出订单快照 + 清理已完工订单状态
        throw new UnsupportedOperationException("TODO(M4-2): 见方法内注释");
    }
}
