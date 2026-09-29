package com.mes.production.monitor.common.enums;

/**
 * 报工通道。旧 MES 有手动 + 扫码两套，新 MES 只有手动，见文档 2.2。
 */
public enum OutputType {

    /** 手动报工：来源 lot 表，取 finishedquantity */
    MANUAL("MES手动报工"),

    /** 过线扫码：来源 barcodeautomatic 表，取 quantity */
    BARCODE("过线扫码");

    private final String label;

    OutputType(String label) {
        this.label = label;
    }

    /** 用于宽表落地的中文口径名，与原报表保持一致 */
    public String getLabel() {
        return label;
    }
}
