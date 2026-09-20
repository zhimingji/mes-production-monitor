package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 分钟级产量指标，对应 ClickHouse 表 {@code dws_report_1min}。
 *
 * <p>这个聚合是可交换可结合的，倾斜时可用<b>两阶段聚合</b>（key 加随机后缀预聚合 -&gt; 去后缀二次聚合）
 * 解决，见文档 11.1。
 */
public class MinuteMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 窗口起始时间（分钟对齐），epoch millis */
    public long tsMinute;

    public String company;
    public String workshop;
    public String lineCode;
    public String lineName;
    public String partCode;

    /** SUM(lotQuantity) */
    public BigDecimal outputQuantity;
    /** 报工笔数 */
    public long recordCnt;

    /** 窗口输出时间戳，作业重启重算窗口时靠它幂等覆盖 */
    public long version;

    public MinuteMetric() {
    }
}
