package com.mes.production.monitor.common.enums;

/**
 * 报工异常三分类。见文档 7.4。
 */
public enum AlertType {

    /**
     * 超报：订单累计报工量 > 订单量。
     * <p>注意：超报<b>无法自动恢复</b>（除非订单量被调大），只能人工关闭，
     * 与滞后/漏报的恢复语义不同，见文档 6.4.3。
     */
    OVER_REPORT(false),

    /** 滞后：实际达成率 < 计划进度 - 阈值。进度追上后可自动恢复。 */
    LAG(true),

    /**
     * 漏报：产线连续 N 分钟无报工。恢复条件是重新出现报工。
     * <p>检测<b>必须用处理时间 Timer</b>：产线停了就没有新数据，watermark 不推进，
     * 事件时间 Timer 永远不触发，见文档 6.4.2。
     */
    MISSING_REPORT(true);

    private final boolean autoRecoverable;

    AlertType(boolean autoRecoverable) {
        this.autoRecoverable = autoRecoverable;
    }

    public boolean isAutoRecoverable() {
        return autoRecoverable;
    }
}
