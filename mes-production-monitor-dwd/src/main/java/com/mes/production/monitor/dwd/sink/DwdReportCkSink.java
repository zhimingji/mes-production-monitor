package com.mes.production.monitor.dwd.sink;

import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.sink.ClickHouseBatchSink;
import com.mes.production.monitor.common.util.ShiftUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * DWD 明细表 ClickHouse Sink。
 *
 * <p>攒批、checkpoint 强制 flush、metric 都在父类 {@link ClickHouseBatchSink} 里，
 * 这里只负责列绑定。
 *
 * <p><b>注意 ClickHouse 的 String 列默认不可为 NULL</b>：直接 setString(null) 会报
 * {@code Cannot convert NULL to non-Nullable type}。所有可能为空的字段都要落成空串，
 * 这是维表 join miss 时最容易撞到的错——数据本身没问题，是维度字段为 null 导致写入失败。
 */
public class DwdReportCkSink extends ClickHouseBatchSink<DwdReportRecord> {

    private static final long serialVersionUID = 1L;

    public DwdReportCkSink(String jdbcUrl, String username, String password,
                           int batchSize, long flushIntervalMs) {
        super(jdbcUrl, username, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO dwd_mes_report ("
                + "record_id, version, company, datasource, outputtype, "
                + "productionorder, productionorderid, lpnbarcode, username, "
                + "partcode, partname, linecode, linename, workcentertype, workshop, "
                + "orderquantity, lotquantity, outputdatetime, shift_date"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    protected void bindRow(PreparedStatement ps, DwdReportRecord r) throws SQLException {
        int i = 1;
        ps.setString(i++, nvl(r.recordId));
        ps.setLong(i++, r.version);
        ps.setString(i++, nvl(r.company));
        ps.setString(i++, nvl(r.dataSource));
        ps.setString(i++, nvl(r.outputType));
        ps.setString(i++, nvl(r.productionOrder));
        ps.setLong(i++, r.productionOrderId);
        ps.setString(i++, nvl(r.lpnBarcode));
        ps.setString(i++, nvl(r.userName));
        ps.setString(i++, nvl(r.partCode));
        ps.setString(i++, nvl(r.partName));
        ps.setString(i++, nvl(r.lineCode));
        ps.setString(i++, nvl(r.lineName));
        ps.setString(i++, nvl(r.workCenterType));
        ps.setString(i++, nvl(r.workshop));
        ps.setLong(i++, r.orderQuantity);
        ps.setBigDecimal(i++, r.lotQuantity == null ? BigDecimal.ZERO : r.lotQuantity);
        ps.setTimestamp(i++, new Timestamp(r.outputDateTime));
        ps.setDate(i, Date.valueOf(ShiftUtil.fromEpochDay(r.shiftDate)));
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
