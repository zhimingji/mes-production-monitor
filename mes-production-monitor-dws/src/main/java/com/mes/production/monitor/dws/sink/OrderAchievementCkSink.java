package com.mes.production.monitor.dws.sink;

import com.mes.production.monitor.common.model.OrderAchievement;
import com.mes.production.monitor.common.sink.ClickHouseBatchSink;
import com.mes.production.monitor.common.util.ShiftUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * 订单级达成率 ClickHouse Sink。
 *
 * <p>同一订单会随着报工不断更新，靠 {@code ReplacingMergeTree(version)} +
 * {@code ORDER BY (company, shift_date, productionorder)} 覆盖旧值，
 * 所以这里每次都是全量 upsert 而不是增量。
 */
public class OrderAchievementCkSink extends ClickHouseBatchSink<OrderAchievement> {

    private static final long serialVersionUID = 1L;

    public OrderAchievementCkSink(String jdbcUrl, String username, String password,
                                  int batchSize, long flushIntervalMs) {
        super(jdbcUrl, username, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO dws_order_achievement ("
                + "shift_date, company, productionorder, productionorderid, workshop, linecode, partcode, "
                + "order_quantity, report_quantity, achieve_rate, plan_progress, last_report_time, version"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    protected void bindRow(PreparedStatement ps, OrderAchievement a) throws SQLException {
        int i = 1;
        ps.setDate(i++, Date.valueOf(ShiftUtil.fromEpochDay(a.shiftDate)));
        ps.setString(i++, nvl(a.company));
        ps.setString(i++, nvl(a.productionOrder));
        ps.setLong(i++, a.productionOrderId);
        ps.setString(i++, nvl(a.workshop));
        ps.setString(i++, nvl(a.lineCode));
        ps.setString(i++, nvl(a.partCode));
        ps.setLong(i++, a.orderQuantity);
        ps.setBigDecimal(i++, zeroIfNull(a.reportQuantity));
        ps.setBigDecimal(i++, zeroIfNull(a.achieveRate));
        ps.setBigDecimal(i++, zeroIfNull(a.planProgress));
        ps.setTimestamp(i++, new Timestamp(a.lastReportTime));
        ps.setLong(i, a.version);
    }

    private static BigDecimal zeroIfNull(BigDecimal v) {
        return v == null ? BigDecimal.ZERO : v;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
