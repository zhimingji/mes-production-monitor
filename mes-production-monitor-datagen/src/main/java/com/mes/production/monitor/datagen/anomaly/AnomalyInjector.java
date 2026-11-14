package com.mes.production.monitor.datagen.anomaly;

import com.mes.production.monitor.datagen.writer.MysqlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 异常注入器。见文档 9.3.1 与 {@link AnomalyConfig}。
 *
 * <p>每个方法对应一个可独立验证的场景。建议的使用方式是<b>一次只开一种</b>，
 * 确认该类告警能被准确捕获后再开下一种；全开着的话出问题时分不清是哪个环节的错。
 *
 * <p>与 {@code ReportGenerator} 的分工：generator 在造数时把「脏数据」（FG 订单号、非正报工量、
 * 悬空外键）就地烘焙进去；本类的「主动异常」（修正/断流/超报/滞后/乱序）是对已造数据的<b>定向修改</b>，
 * 需要后置注入才能复现。
 */
public class AnomalyInjector {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyInjector.class);

    private static final String LOT_INSERT_SQL = "INSERT IGNORE INTO lot "
            + "(lotid, lotno, productionorderid, workcenterid, finishedquantity, createddate, createdbyid) "
            + "VALUES (?,?,?,?,?,?,?)";

    private final MysqlWriter writer;
    private final AnomalyConfig config;

    public AnomalyInjector(MysqlWriter writer, AnomalyConfig config) {
        this.writer = writer;
        this.config = config;
    }

    /**
     * 注入报工修正：UPDATE 已有 lot.finishedquantity（按 {@code correctionRatio} 比例，用取模选中）。
     *
     * <p><b>这是验证 changelog 语义的关键一步。</b>验证清单：
     * <ol>
     *   <li>Kafka {@code ods-mes-report} 里能看到 {@code op=u} 的消息，且 {@code before} 有旧值
     *       —— 如果 before 是空的，检查 MySQL 的 {@code binlog_row_image} 是不是 FULL。</li>
     *   <li>ClickHouse 明细表里该 {@code record_id} 只有一条有效记录（查询要加 FINAL 或用视图）。</li>
     *   <li>对账时该订单的产量等于修正后的值，不是修正前 + 修正后。</li>
     * </ol>
     */
    public void injectCorrection(String db) {
        int modulus = (int) Math.max(1, Math.round(1.0 / config.correctionRatio));
        double factor = 1.0 + config.correctionMagnitude;
        int n = writer.execute(db,
                "UPDATE lot SET finishedquantity = ROUND(finishedquantity * ?, 4) WHERE lotid % ? = 0",
                factor, modulus);
        LOG.info("[{}] 注入报工修正完成: 修正 {} 条", db, n);
    }

    /**
     * 注入产线断流：让指定产线在接下来的 N 分钟内不产生任何报工。
     *
     * <p>断流是流式语义——「没有数据」无法用一条 UPDATE 表达。这里以真实时间循环：
     * 持续给<b>非目标线</b>写报工（保持链路/watermark 活跃），目标线一条不写；
     * 到点后给目标线各写一条（恢复）再退出。目标线选总装线，与真实扫码/报工重心一致。
     *
     * <p><b>这是本项目最有价值的一个验证场景。</b>推荐刻意分两步：先用事件时间 Timer 实现漏报检测，
     * 跑这个注入观察告警没出来；再改成处理时间 Timer，告警正常触发。见文档 6.4.2。
     */
    public void injectLineIdle(String db, int lineCount, int durationMinutes) {
        List<Object[]> target = writer.query(db,
                "SELECT workcenterid FROM workcenter WHERE workcentertype = '总装' "
                        + "ORDER BY workcenterid LIMIT " + lineCount);
        if (target.isEmpty()) {
            LOG.warn("[{}] 无总装线可停写，跳过断流注入", db);
            return;
        }
        List<Object[]> active = writer.query(db,
                "SELECT workcenterid FROM workcenter WHERE workcentertype <> '总装' "
                        + "ORDER BY workcenterid LIMIT 1");
        long activeLine = ((Number) active.get(0)[0]).longValue();
        long orderId = ((Number) writer.query(db,
                "SELECT productionorderid FROM productionorder LIMIT 1").get(0)[0]).longValue();
        long idBase = System.currentTimeMillis() * 1000L;

        long end = System.currentTimeMillis() + durationMinutes * 60_000L;
        long seq = 0;
        LOG.info("[{}] 注入断流: {} 条产线停写 {} 分钟", db, target.size(), durationMinutes);
        while (System.currentTimeMillis() < end) {
            writer.batch(db, LOT_INSERT_SQL, Collections.singletonList(new Object[]{
                    idBase + (++seq), "LOT-IDLE-" + seq, orderId, activeLine,
                    BigDecimal.ONE, LocalDateTime.now(), 1L}));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // 恢复：给目标线各写一条，验证 MISSING_REPORT 的恢复语义
        List<Object[]> recovery = new ArrayList<>(target.size());
        for (Object[] l : target) {
            recovery.add(new Object[]{idBase + (++seq), "LOT-IDLE-REC-" + seq, orderId,
                    ((Number) l[0]).longValue(), BigDecimal.ONE, LocalDateTime.now(), 1L});
        }
        writer.batch(db, LOT_INSERT_SQL, recovery);
        LOG.info("[{}] 断流注入完成，已恢复 {} 条产线", db, target.size());
    }

    /**
     * 注入超报：给部分订单各插一条 {@code finishedquantity = orderquantity × (1 + excess)} 的大 lot，
     * 单这一条就足以让累计报工量超过订单量。验证 OVER_REPORT 告警 + 「超报无法自动恢复」语义（文档 6.4.3）。
     */
    public void injectOverReport(String db) {
        List<Object[]> orders = writer.query(db,
                "SELECT productionorderid, orderquantity FROM productionorder ORDER BY productionorderid");
        int target = Math.max(1, (int) Math.round(orders.size() * config.overReportOrderRatio));
        long idBase = System.currentTimeMillis() * 1000L;
        List<Object[]> rows = new ArrayList<>(target);
        for (int i = 0; i < target; i++) {
            long orderId = ((Number) orders.get(i)[0]).longValue();
            long orderQty = ((Number) orders.get(i)[1]).longValue();
            BigDecimal overQty = BigDecimal.valueOf(orderQty)
                    .multiply(BigDecimal.valueOf(1.0 + config.overReportExcessRatio));
            rows.add(new Object[]{idBase + i, "LOT-OVER-" + orderId, orderId, 1L,
                    overQty, LocalDateTime.now(), 1L});
        }
        writer.batch(db, LOT_INSERT_SQL, rows);
        LOG.info("[{}] 注入超报完成: {} 单", db, rows.size());
    }

    /**
     * 注入进度滞后：把部分订单的 duedate 提前到 createddate 后 2 小时。
     * 计划周期被压缩到 2 小时，而订单是几天前创建的，计划进度立刻到 100%，
     * 达成率还停留在低位 → LAG。验证 LAG 告警 + P3→P2→P1 分级升级 + 追上进度后的恢复。
     */
    public void injectLag(String db) {
        List<Object[]> orders = writer.query(db,
                "SELECT productionorderid FROM productionorder ORDER BY productionorderid");
        int target = Math.max(1, (int) Math.round(orders.size() * config.lagOrderRatio));
        int n = 0;
        for (int i = 0; i < target; i++) {
            long orderId = ((Number) orders.get(i)[0]).longValue();
            n += writer.execute(db,
                    "UPDATE productionorder SET duedate = DATE_ADD(createddate, INTERVAL 2 HOUR) "
                            + "WHERE productionorderid = ?", orderId);
        }
        LOG.info("[{}] 注入滞后完成: {} 单", db, n);
    }

    /**
     * 注入乱序：把部分记录的 createddate 回拨若干秒，回拨幅度用取模分布、
     * 覆盖 {@code [minSeconds, maxSeconds]}，故意<b>跨过</b> watermark 容忍度（默认 60s）：
     * 小于 60s 的验证「能正常等到」，大于 60s 的验证「迟到走 side output 且被计数」。
     */
    public void injectOutOfOrder(String db) {
        int modulus = (int) Math.max(1, Math.round(1.0 / config.outOfOrderRatio));
        int span = config.outOfOrderMaxSeconds - config.outOfOrderMinSeconds + 1;
        int n = writer.execute(db,
                "UPDATE lot SET createddate = DATE_SUB(createddate, "
                        + "INTERVAL ((lotid % ?) + ?) SECOND) WHERE lotid % ? = 0",
                span, config.outOfOrderMinSeconds, modulus);
        LOG.info("[{}] 注入乱序完成: 回拨 {} 条", db, n);
    }

    public AnomalyConfig config() {
        return config;
    }
}
