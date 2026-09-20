package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * DWD 统一报工明细宽表。字段与 ClickHouse 表 {@code dwd_mes_report} 一一对应，见文档 5.2 / 5.3。
 *
 * <p><b>DWD 层不做聚合</b>：扫码通道的 LPN 聚合边界在流上没有天然答案，
 * 因此保持明细、把边界交给 DWS 的窗口显式定义，见文档 6.2.2。
 */
public class DwdReportRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 业务唯一键：lotid 或 barcode。ClickHouse ReplacingMergeTree 的去重键 */
    public String recordId;

    /** 版本号：CDC ts_ms。同 recordId 取最大值，实现"报工修正取最新" */
    public long version;

    public String company;
    public String dataSource;
    public String outputType;

    /** 对齐后的订单编号（两通道统一到这个键） */
    public String productionOrder;
    /** 订单主键。DWS 达成率要先按订单聚合，必须保留，见文档 7.2 */
    public long productionOrderId;

    public String lpnBarcode;
    public String userName;
    public String partCode;
    public String partName;
    public String lineCode;
    public String lineName;
    public String workCenterType;
    public String workshop;

    /** 订单计划量。订单级属性，维度聚合时不能直接 SUM，见文档 7.2 */
    public long orderQuantity;
    /** 报工量（实际） */
    public BigDecimal lotQuantity;

    /** 报工时间（事件时间），epoch millis */
    public long outputDateTime;
    /** 班次日（08:30 日界换算后的归属日期），epoch day。见文档 6.3.1 */
    public int shiftDate;

    public DwdReportRecord() {
    }

    @Override
    public String toString() {
        return "DwdReportRecord{recordId=" + recordId + ", company=" + company
                + ", order=" + productionOrder + ", line=" + lineCode
                + ", qty=" + lotQuantity + ", shiftDate=" + shiftDate + '}';
    }
}
