package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 告警记录，对应 ClickHouse 表 {@code ads_alert}。见文档 5.3 / 7.4。
 *
 * <p>告警去重：同一 {@link #alertId}（建议 = type + company + linecode + order 的哈希）
 * 在升级时更新原记录而不是新增，避免刷屏。
 */
public class AlertRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 告警唯一标识，升级时保持不变 */
    public String alertId;

    /** OVER_REPORT / LAG / MISSING_REPORT */
    public String alertType;
    /** NONE / P3 / P2 / P1 */
    public String level;

    public String company;
    public String lineCode;
    public String productionOrder;

    public long orderQuantity;
    public BigDecimal reportQuantity;
    public BigDecimal achieveRate;

    /** 首次触发时间，epoch millis */
    public long alertTime;
    /**
     * 恢复时间，epoch millis；0 表示未恢复。
     * <p>注意 OVER_REPORT 无法自动恢复，见 {@link com.mes.production.monitor.common.enums.AlertType}。
     */
    public long recoverTime;

    public long version;

    public AlertRecord() {
    }
}
