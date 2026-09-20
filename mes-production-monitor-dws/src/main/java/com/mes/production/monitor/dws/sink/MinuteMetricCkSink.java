package com.mes.production.monitor.dws.sink;

import com.mes.production.monitor.common.model.MinuteMetric;
import com.mes.production.monitor.common.sink.ClickHouseBatchSink;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/** 分钟级产量指标 ClickHouse Sink */
public class MinuteMetricCkSink extends ClickHouseBatchSink<MinuteMetric> {

    private static final long serialVersionUID = 1L;

    public MinuteMetricCkSink(String jdbcUrl, String username, String password,
                              int batchSize, long flushIntervalMs) {
        super(jdbcUrl, username, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO dws_report_1min ("
                + "ts_minute, company, workshop, linecode, linename, partcode, "
                + "output_quantity, record_cnt, version"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    protected void bindRow(PreparedStatement ps, MinuteMetric m) throws SQLException {
        int i = 1;
        ps.setTimestamp(i++, new Timestamp(m.tsMinute));
        ps.setString(i++, nvl(m.company));
        ps.setString(i++, nvl(m.workshop));
        ps.setString(i++, nvl(m.lineCode));
        ps.setString(i++, nvl(m.lineName));
        ps.setString(i++, nvl(m.partCode));
        ps.setBigDecimal(i++, m.outputQuantity == null ? BigDecimal.ZERO : m.outputQuantity);
        ps.setLong(i++, m.recordCnt);
        ps.setLong(i, m.version);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
