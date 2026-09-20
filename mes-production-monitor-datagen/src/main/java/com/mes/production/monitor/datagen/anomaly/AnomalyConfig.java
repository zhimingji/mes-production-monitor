package com.mes.production.monitor.datagen.anomaly;

import com.mes.production.monitor.common.util.JobConfig;

import java.io.Serializable;

/**
 * 异常注入配置。见文档 9.3.1。
 *
 * <p><b>为什么这个类很重要</b>：三类告警如果造出来的数据全是正常生产，告警永远不触发，
 * 正确性无从验证。这套注入能力做出来，你就有了一个<b>可复现的验收环境</b>——
 * 每次改完告警逻辑，跑一遍注入就知道对不对。
 *
 * <p>这件事本身就是简历上的一句话：「为验证实时链路正确性，实现了可配置的异常注入造数器，
 * 支持定向复现超报/断流/乱序/修正四类场景」。
 */
public class AnomalyConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ==================== 超报：验证 OVER_REPORT ====================
    /** 多少比例的订单会被造成超报 */
    public double overReportOrderRatio;
    /** 超报幅度：报工量 = 订单量 × (1 + 这个值) */
    public double overReportExcessRatio;

    // ==================== 漏报：验证 MISSING_REPORT + Timer 时间语义 ====================
    /** 停写的产线数量 */
    public int idleLineCount;
    /** 停写时长（分钟）。必须大于 alert.idle.threshold.ms 才会触发告警 */
    public int idleDurationMinutes;

    // ==================== 滞后：验证 LAG + 分级升级 ====================
    /** 多少比例的订单进度落后于计划爬坡 */
    public double lagOrderRatio;
    /** 落后幅度：实际进度 = 计划进度 × (1 - 这个值) */
    public double lagBehindRatio;

    // ==================== 乱序：验证 watermark 容忍度与迟到处理 ====================
    /** 多少比例的记录时间戳被回拨 */
    public double outOfOrderRatio;
    /** 回拨范围（秒）。设成大于 watermark.out.of.orderness.seconds 才能造出真正的迟到数据 */
    public int outOfOrderMinSeconds;
    public int outOfOrderMaxSeconds;

    // ==================== 报工修正：验证 changelog 语义与 CK 去重 ====================
    /** 多少比例的 lot 记录会被 UPDATE 修正 */
    public double correctionRatio;
    /** 修正幅度：新值 = 旧值 × (1 ± 这个值) */
    public double correctionMagnitude;

    // ==================== 脏数据：验证过滤口径与脏数据计数 ====================
    /** 多少比例的订单号以 FG 开头（原报表口径要过滤掉） */
    public double fgOrderRatio;
    /** 多少比例的报工量 <= 0 */
    public double nonPositiveQtyRatio;
    /** 多少比例的记录引用不存在的维表外键（验证 dim_miss_count） */
    public double danglingFkRatio;

    public AnomalyConfig() {
    }

    public static AnomalyConfig from(JobConfig conf) {
        AnomalyConfig c = new AnomalyConfig();
        c.overReportOrderRatio = conf.getDouble("anomaly.over.report.order.ratio", 0.02);
        c.overReportExcessRatio = conf.getDouble("anomaly.over.report.excess.ratio", 0.1);
        c.idleLineCount = conf.getInt("anomaly.idle.line.count", 2);
        c.idleDurationMinutes = conf.getInt("anomaly.idle.duration.minutes", 90);
        c.lagOrderRatio = conf.getDouble("anomaly.lag.order.ratio", 0.05);
        c.lagBehindRatio = conf.getDouble("anomaly.lag.behind.ratio", 0.4);
        c.outOfOrderRatio = conf.getDouble("anomaly.out.of.order.ratio", 0.03);
        c.outOfOrderMinSeconds = conf.getInt("anomaly.out.of.order.min.seconds", 10);
        c.outOfOrderMaxSeconds = conf.getInt("anomaly.out.of.order.max.seconds", 120);
        c.correctionRatio = conf.getDouble("anomaly.correction.ratio", 0.03);
        c.correctionMagnitude = conf.getDouble("anomaly.correction.magnitude", 0.2);
        c.fgOrderRatio = conf.getDouble("anomaly.fg.order.ratio", 0.03);
        c.nonPositiveQtyRatio = conf.getDouble("anomaly.non.positive.qty.ratio", 0.02);
        c.danglingFkRatio = conf.getDouble("anomaly.dangling.fk.ratio", 0.005);
        return c;
    }
}
