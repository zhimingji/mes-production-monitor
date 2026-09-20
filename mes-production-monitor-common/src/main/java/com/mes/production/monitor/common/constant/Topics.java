package com.mes.production.monitor.common.constant;

/**
 * Kafka Topic 定义。见 技术设计文档.md 5.1 节。
 */
public final class Topics {

    /** ODS：CDC 原始报工明细（debezium-json，保留完整 changelog） */
    public static final String ODS_MES_REPORT = "ods-mes-report";

    /** DWD：清洗 + 订单关联 + 维表关联后的统一宽表 */
    public static final String DWD_MES_REPORT = "dwd-mes-report";

    /** ADS：告警消息 */
    public static final String MES_ALERT = "mes-alert";

    private Topics() {
    }
}
