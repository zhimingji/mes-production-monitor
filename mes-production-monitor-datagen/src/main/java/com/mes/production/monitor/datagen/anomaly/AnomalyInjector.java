package com.mes.production.monitor.datagen.anomaly;

import com.mes.production.monitor.datagen.writer.MysqlWriter;

/**
 * 异常注入器。见文档 9.3.1 与 {@link AnomalyConfig}。
 *
 * <p>每个方法对应一个可独立验证的场景。建议的使用方式是<b>一次只开一种</b>，
 * 确认该类告警能被准确捕获后再开下一种；全开着的话出问题时分不清是哪个环节的错。
 */
public class AnomalyInjector {

    private final MysqlWriter writer;
    private final AnomalyConfig config;

    public AnomalyInjector(MysqlWriter writer, AnomalyConfig config) {
        this.writer = writer;
        this.config = config;
    }

    /**
     * TODO(M1.5-1) 注入报工修正：UPDATE 已有的 lot.finishedquantity。
     *
     * <p><b>这是验证 changelog 语义的关键一步。</b>验证清单：
     * <ol>
     *   <li>Kafka {@code ods-mes-report} 里能看到 {@code op=u} 的消息，且 {@code before} 有旧值
     *       —— 如果 before 是空的，检查 MySQL 的 {@code binlog_row_image} 是不是 FULL。</li>
     *   <li>ClickHouse 明细表里该 {@code record_id} 只有一条有效记录（查询要加 FINAL 或用视图）。</li>
     *   <li>对账时该订单的产量等于修正后的值，不是修正前 + 修正后。</li>
     * </ol>
     * 第 3 条如果不满足，说明 CDC sink 走了"只取新值"的路线 C（文档 6.1.1），
     * 或者聚合层没处理同 record_id 的重复累加。
     */
    public void injectCorrection(String db) {
        throw new UnsupportedOperationException("TODO(M1.5-1): 见方法注释");
    }

    /**
     * TODO(M1.5-2) 注入产线断流：让指定产线在接下来的 N 分钟内不产生任何报工。
     *
     * <p><b>这是本项目最有价值的一个验证场景。</b>推荐的做法是刻意分两步：
     * <ol>
     *   <li>先用<b>事件时间</b> Timer 实现漏报检测，跑这个注入，
     *       观察告警<b>确实没有出来</b>——因为没有新数据，watermark 不推进，Timer 永不触发。</li>
     *   <li>再改成<b>处理时间</b> Timer，重跑，告警正常触发。</li>
     * </ol>
     * 亲手看到这个现象一次，比读十篇文章记得牢。见文档 6.4.2。
     */
    public void injectLineIdle(String db, int lineCount, int durationMinutes) {
        throw new UnsupportedOperationException("TODO(M1.5-2): 见方法注释");
    }

    /**
     * TODO(M1.5-3) 注入超报：让某订单的累计报工量超过 orderquantity。
     * 验证 OVER_REPORT 告警，以及"超报无法自动恢复"的语义（文档 6.4.3）。
     */
    public void injectOverReport(String db) {
        throw new UnsupportedOperationException("TODO(M1.5-3): 见方法注释");
    }

    /**
     * TODO(M1.5-4) 注入进度滞后：让某订单的报工进度明显落后于计划爬坡曲线。
     * 验证 LAG 告警 + P3→P2→P1 分级升级 + 追上进度后的恢复。
     */
    public void injectLag(String db) {
        throw new UnsupportedOperationException("TODO(M1.5-4): 见方法注释");
    }

    /**
     * TODO(M1.5-5) 注入乱序：把部分记录的 createddate / scantime 回拨若干秒。
     *
     * <p>回拨幅度要<b>跨过</b> watermark 容忍度（默认 60s）：
     * 小于 60s 的验证"能正常等到"，大于 60s 的验证"迟到走 side output 且被计数"。
     * 两种都要造，否则只测了一半。
     */
    public void injectOutOfOrder(String db) {
        throw new UnsupportedOperationException("TODO(M1.5-5): 见方法注释");
    }

    public AnomalyConfig config() {
        return config;
    }
}
