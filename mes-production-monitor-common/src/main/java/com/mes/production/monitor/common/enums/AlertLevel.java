package com.mes.production.monitor.common.enums;

/**
 * 告警级别。分级升级链路：NONE -> P3 -> P2 -> P1，见文档 7.4。
 */
public enum AlertLevel {

    /** 无告警 / 已恢复 */
    NONE(0),

    /** P3 通知：首次触发 */
    P3(3),

    /** P2 升级：连续 N 个窗口未恢复 */
    P2(2),

    /** P1 严重：再次升级后的终态，不再继续升级 */
    P1(1);

    private final int severity;

    AlertLevel(int severity) {
        this.severity = severity;
    }

    public int getSeverity() {
        return severity;
    }

    /** 升级到下一级；已是 P1 则保持 P1（终态） */
    public AlertLevel escalate() {
        switch (this) {
            case NONE:
                return P3;
            case P3:
                return P2;
            case P2:
            case P1:
            default:
                return P1;
        }
    }
}
