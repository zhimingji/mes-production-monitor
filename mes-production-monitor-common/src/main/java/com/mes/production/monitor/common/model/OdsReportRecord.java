package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * ODS 层统一报工记录：两个通道（lot 手动报工 / barcodeautomatic 过线扫码）解析后的统一载体。
 *
 * <p><b>Flink POJO 规范</b>：public 类 + public 无参构造 + public 字段。不满足会静默退化成 Kryo
 * 序列化，性能差好几倍且不报错。开发期在 env 上调 {@code getConfig().disableGenericTypes()}
 * 把问题暴露出来，见文档 6.2.3。
 *
 * <p>字段是两个通道的并集，按 {@link #outputType} 区分哪些字段有效。
 */
public class OdsReportRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    // ==================== changelog 元信息（见文档 6.1.1） ====================

    /**
     * 变更类型：{@code c}=insert / {@code u}=update / {@code d}=delete / {@code r}=snapshot read。
     * <p>Debezium 的 op 字段。{@code u} 的记录同时带 before 和 after，
     * 是"报工修正不重复累加"的关键——丢了 before 就只能看到新值，累加必然偏大。
     */
    public String op;

    /** binlog 事件时间戳（Debezium ts_ms），用作 ClickHouse ReplacingMergeTree 的 version */
    public long tsMs;

    /** 源库名，如 mes_old_zh。company / datasource 由它解析 */
    public String databaseName;

    /** 源表名：lot 或 barcodeautomatic */
    public String tableName;

    // ==================== 打标签后的维度 ====================

    /** 基地标识：zh / zz / hf / wh / cq */
    public String company;

    /** OLD_MES / NEW_MES */
    public String dataSource;

    /** MANUAL / BARCODE */
    public String outputType;

    /** 业务唯一键：手动报工 = lotid，扫码 = barcode。CK 去重靠它，见文档 5.3 */
    public String recordId;

    // ==================== 两个通道共有的订单键 ====================

    /**
     * 生产订单 ID。lot 与 barcodeautomatic 均通过该字段关联
     * productionorder.productionorderid，取得真实订单编号 productionorderno。
     */
    public Long productionOrderId;

    // ==================== lot 通道字段 ====================

    public Long lotId;
    public String lotNo;
    public BigDecimal finishedQuantity;
    /** 报工时间（事件时间），epoch millis */
    public Long createdDate;
    public Long createdById;

    // ==================== barcodeautomatic 通道字段 ====================

    public String barcode;
    public String lpnBarcode;
    public String modelEncode;
    public String model;
    public String users;
    public Integer quantity;
    /** 扫描时间（事件时间），epoch millis */
    public Long scanTime;

    public OdsReportRecord() {
    }

    /** 统一取事件时间：手动报工用 createdDate，扫码用 scanTime */
    public long eventTime() {
        if (createdDate != null) {
            return createdDate;
        }
        return scanTime == null ? 0L : scanTime;
    }

    @Override
    public String toString() {
        return "OdsReportRecord{op=" + op + ", db=" + databaseName + ", table=" + tableName
                + ", recordId=" + recordId + ", outputType=" + outputType + ", tsMs=" + tsMs + '}';
    }
}
