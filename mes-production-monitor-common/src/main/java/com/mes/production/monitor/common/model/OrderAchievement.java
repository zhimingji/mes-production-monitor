package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单级达成率，对应 ClickHouse 表 {@code dws_order_achievement}。
 *
 * <p><b>为什么落到订单粒度而不是维度粒度</b>：订单量是订单级属性，直接
 * {@code SUM(报工)/SUM(订单量)} 会把同一订单的 orderQuantity 按报工笔数重复累加。
 * 正确做法是先按订单聚合（本类），维度级达成率交给 ClickHouse 查询侧现算——
 * 这张表已经是订单去重的，维度 SUM 不会重复累加。见文档 7.2 / 6.3.2。
 *
 * <p>这样也绕开了流上二级聚合的 retract 复杂度：一级聚合的输出是持续更新的值，
 * 在 Flink 里做二级聚合必须处理回撤流。
 */
public class OrderAchievement implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 班次日，epoch day */
    public int shiftDate;

    public String company;
    public String productionOrder;
    public long productionOrderId;
    public String workshop;
    public String lineCode;
    public String partCode;

    /** 订单计划量 */
    public long orderQuantity;
    /** 该订单累计报工量 */
    public BigDecimal reportQuantity;
    /** reportQuantity / orderQuantity */
    public BigDecimal achieveRate;
    /** 计划进度（线性爬坡）：已用时间 / 生产周期，见文档 7.3 */
    public BigDecimal planProgress;

    /** 最后一次报工时间，epoch millis */
    public long lastReportTime;

    public long version;

    public OrderAchievement() {
    }

    /** 进度偏差 = 达成率 - 计划进度。负值表示滞后，是 LAG 告警的依据 */
    public BigDecimal progressDeviation() {
        if (achieveRate == null || planProgress == null) {
            return BigDecimal.ZERO;
        }
        return achieveRate.subtract(planProgress);
    }
}
