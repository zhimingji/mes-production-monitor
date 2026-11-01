package com.mes.production.monitor.dwd.sink;

import com.mes.production.monitor.common.model.OdsReportRecord;
import com.mes.production.monitor.common.sink.ClickHouseBatchSink;
import com.mes.production.monitor.common.util.JsonUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 报工物理删除审计 Sink：把每条 {@code op=d} 的完整 before 落 CK 审计表
 * {@code dwd_mes_report_deleted_audit}，保证墓碑方案演进时物理删除仍可回溯。
 *
 * <p><b>payload 存整条 before 的 JSON，不做列级解析</b>：审计表的职责是"原样留证"，
 * 源库加字段/改结构都不影响这里。定位问题靠 record_id + company + ts_ms 三列索引。
 *
 * <p>审计表是 append-only {@code MergeTree}，不依赖 ReplacingMergeTree 去重——
 * at-least-once 重放可能重复写，审计场景可接受（宁可多留一条，不可少留一条）。
 */
public class DeletedReportAuditSink extends ClickHouseBatchSink<OdsReportRecord> {

    private static final long serialVersionUID = 1L;

    public DeletedReportAuditSink(String jdbcUrl, String username, String password,
                                  int batchSize, long flushIntervalMs) {
        super(jdbcUrl, username, password, batchSize, flushIntervalMs);
    }

    @Override
    protected String insertSql() {
        return "INSERT INTO dwd_mes_report_deleted_audit "
                + "(record_id, company, table_name, ts_ms, payload, audit_time) "
                + "VALUES (?, ?, ?, ?, ?, now())";
    }

    @Override
    protected void bindRow(PreparedStatement ps, OdsReportRecord r) throws SQLException {
        ps.setString(1, r.recordId == null ? "" : r.recordId);
        ps.setString(2, r.company == null ? "" : r.company);
        ps.setString(3, r.tableName == null ? "" : r.tableName);
        ps.setLong(4, r.tsMs);
        ps.setString(5, JsonUtil.toJson(r));
    }
}
