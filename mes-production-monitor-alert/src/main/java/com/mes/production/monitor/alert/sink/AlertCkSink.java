package com.mes.production.monitor.alert.sink;

import com.mes.production.monitor.common.model.AlertRecord;
import com.mes.production.monitor.common.sink.ClickHouseBatchSink;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * 告警记录 ClickHouse Sink。
 *
 * <p>升级时保持 {@code alert_id} 不变、只更新 level 与 version，
 * 靠 {@code ReplacingMergeTree(version)} 覆盖，这样大屏看到的是一条告警的当前状态，
 * 而不是 P3/P2/P1 三条重复记录。
 */
public class AlertCkSink extends ClickHouseBatchSink<AlertRecord> {

    private static final long serialVersionUID = 1L;

    public AlertCkSink(String jdbcUrl, String username, String password,
                       int batchSize, long flushIntervalMs) {
        super(jdbcUrl, username, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO ads_alert ("
                + "alert_id, alert_type, level, company, linecode, productionorder, "
                + "order_quantity, report_quantity, achieve_rate, alert_time, recover_time, version"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    protected void bindRow(PreparedStatement ps, AlertRecord a) throws SQLException {
        int i = 1;
        ps.setString(i++, nvl(a.alertId));
        ps.setString(i++, nvl(a.alertType));
        ps.setString(i++, nvl(a.level));
        ps.setString(i++, nvl(a.company));
        ps.setString(i++, nvl(a.lineCode));
        ps.setString(i++, nvl(a.productionOrder));
        ps.setLong(i++, a.orderQuantity);
        ps.setBigDecimal(i++, a.reportQuantity == null ? BigDecimal.ZERO : a.reportQuantity);
        ps.setBigDecimal(i++, a.achieveRate == null ? BigDecimal.ZERO : a.achieveRate);
        ps.setTimestamp(i++, new Timestamp(a.alertTime));
        // recover_time 是 Nullable(DateTime)，未恢复时必须写 NULL 而不是 0（1970 年会污染大屏）
        if (a.recoverTime > 0) {
            ps.setTimestamp(i++, new Timestamp(a.recoverTime));
        } else {
            ps.setNull(i++, Types.TIMESTAMP);
        }
        ps.setLong(i, a.version);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
