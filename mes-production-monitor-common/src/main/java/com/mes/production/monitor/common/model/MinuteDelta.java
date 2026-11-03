package com.mes.production.monitor.common.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 分钟聚合的带符号增量。由 {@code ChangelogNormalizer} 产出，直接进
 * {@code SummingMergeTree} 累加：报工修正/删除用负数回撤，CK 在 merge 时自动求和。
 *
 * <p>为什么不用"窗口内取 version 最大去重"：那只能处理窗口内的修正，迟到修正/删除
 * 窗口已关就无法回撤。带符号增量把"每条记录怎么变"和"按维度怎么加"彻底解耦，
 * 迟到数据只是一条普通 INSERT，见文档 6.3。
 */
public class MinuteDelta implements Serializable {

    private static final long serialVersionUID = 1L;

    public String company;
    public String workshop;
    public String lineCode;
    public String lineName;
    public String partCode;

    /** 报工量增量（可为负 = 回撤） */
    public BigDecimal quantity;

    /** 报工笔数增量（可为负：新增 +1，回撤 -1） */
    public long count;

    /** 事件时间（= 原报工 outputDateTime，窗口归属用） */
    public long eventTime;

    public MinuteDelta() {
    }
}
