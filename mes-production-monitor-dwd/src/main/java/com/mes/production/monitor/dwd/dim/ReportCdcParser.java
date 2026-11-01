package com.mes.production.monitor.dwd.dim;

import com.fasterxml.jackson.databind.JsonNode;
import com.mes.production.monitor.common.constant.MesConstants;
import com.mes.production.monitor.common.enums.DataSourceType;
import com.mes.production.monitor.common.enums.OutputType;
import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.OdsReportRecord;
import com.mes.production.monitor.common.util.JsonUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 解析 {@code ods-mes-report} 的 debezium-json，转成统一 {@link OdsReportRecord}
 *
 * <p>debezium-json sink 只写 {@code before / after / op} 三处，不写 {@code source / ts_ms}。这里取：
 * <ul>
 *   <li><b>信封顶层</b> {@code op}（c/u/d/r）；</li>
 *   <li><b>after / before</b>（delete 时 after 为 null，整行在 before 里）中的业务列，
 *       这些列是 CDC 作业 {@code INSERT ... SELECT} 显式写进去的，字段名即源表 snake_case；
 *       其中 {@code op_ts}（CDC opl_ts 元数据）用作 ClickHouse ReplacingMergeTree 的 version；</li>
 *   <li>库名/表名从 {@code db_name / tbl_name} 读，不读 {@code source.db}——
 *       debezium-json sink 不写 {@code source}，读它恒为 null，
 *       而 {@code db_name} 是实打实 SELECT 出来的普通列，一定在。</li>
 * </ul>
 *
 * <p>解析失败计入 {@link MetricNames#DIRTY_RECORD_COUNT} 后丢弃，不抛异常让作业挂掉。
 *
 * <p>与 {@link OrderCdcParser} 的区别：那个解析订单维、输出 {@code DimUpdate} 进广播流；
 * 本类解析报工事实、输出 {@code OdsReportRecord} 进主数据流。
 */
public class ReportCdcParser extends RichFlatMapFunction<String, OdsReportRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReportCdcParser.class);

    /** 兼容 debezium 两种时间写法：带偏移的 ISO-8601（含 T）与 {@code yyyy-MM-dd HH:mm:ss[.SSS]} */
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");

    private transient Counter dirtyCount;

    @Override
    public void open(Configuration parameters) {
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
        this.dirtyCount = g.counter(MetricNames.DIRTY_RECORD_COUNT);
    }

    @Override
    public void flatMap(String json, Collector<OdsReportRecord> out) {
        try {
            JsonNode root = JsonUtil.readTree(json);

            // op 在信封顶层。不读信封的 ts_ms——debezium-json sink 只写 before/after/op，
            // 不写 ts_ms/source，version 改用 after/before 里的 op_ts（见下方）
            String op = JsonUtil.text(root, "op");
            if (op == null) {
                dirtyCount.inc();
                return;
            }

            // c/r/u -> after；d -> before（delete 时 after 为 null，删除前整行在 before 里）
            JsonNode row;
            if ("d".equals(op)) {
                row = root.get("before");
            } else if ("c".equals(op) || "r".equals(op) || "u".equals(op)) {
                row = root.get("after");
            } else {
                // t（truncate）/ m（message）等：不是报工行级变更，计入脏数据
                dirtyCount.inc();
                return;
            }

            if (row == null || row.isNull()) {
                dirtyCount.inc();
                return;
            }

            OdsReportRecord r = toReport(row);
            // op 来自信封顶层；tsMs（version）来自本行的 op_ts（CDC opl_ts 元数据）。
            // 全量 snapshot 阶段 op_ts 为 epoch 0，属预期：快照行 version=0，增量事件才有真实 binlog 时间
            r.op = op;
            r.tsMs = parseOpTs(JsonUtil.text(row, "op_ts"));

            // 下游 join 要靠这三样定位，缺失时丢弃并计数，别让空 recordId 流下去污染宽表
            if (r.recordId == null || r.databaseName == null || r.tableName == null) {
                dirtyCount.inc();
                return;
            }

            out.collect(r);
        } catch (Exception e) {
            dirtyCount.inc();
            LOG.warn("报工 CDC 解析失败，已丢弃", e);
        }
    }

    /**
     * 把 after/before 里的业务列映射成 {@link OdsReportRecord}。
     * 只做字段搬运，不填 {@code op/tsMs}——那两个由调用方补：
     * {@code op} 来自信封顶层，{@code tsMs} 来自本行的 {@code op_ts}。
     *
     * <p>字段名是 CDC 作业 {@code SELECT} 出来的源表列名（snake_case），
     * 不是 Java 驼峰，见 {@code CdcToKafkaJob.reportKafkaSinkDdl}。
     */
    private static OdsReportRecord toReport(JsonNode row) {
        OdsReportRecord r = new OdsReportRecord();

        // 源身份：db_name / tbl_name 是 CDC 显式写出的普通列，直接读
        r.databaseName = JsonUtil.text(row, "db_name");
        r.tableName = JsonUtil.text(row, "tbl_name");

        // company / datasource 由库名解析，不读 CDC 预计算的 company/datasource——DWD 层只认库名一个真源
        r.company = DataSourceType.companyFromDatabaseName(r.databaseName);
        DataSourceType ds = DataSourceType.fromDatabaseName(r.databaseName);
        r.dataSource = ds == null ? null : ds.name();

        // outputType 由表名推导：after.outputtype 是中文 label（"手动报工"），不是这里要的 MANUAL/BARCODE
        r.outputType = outputTypeOf(r.tableName);

        // 业务唯一键：CDC 已算好 record_id（lot -> CAST(lotid AS STRING)，barcode -> barcode）
        r.recordId = JsonUtil.text(row, "record_id");

        // 订单键（两通道共有）
        r.productionOrderId = JsonUtil.asLong(row, "productionorderid");

        // ---------- lot 通道 ----------
        r.lotId = JsonUtil.asLong(row, "lotid");
        r.lotNo = JsonUtil.text(row, "lotno");
        r.workCenterId = JsonUtil.asLong(row, "workcenterid");
        r.finishedQuantity = JsonUtil.asBigDecimal(row, "finishedquantity");
        r.createdDate = parseMillis(JsonUtil.text(row, "createddate"));
        r.createdById = JsonUtil.asLong(row, "createdbyid");

        // ---------- barcode 通道 ----------
        r.barcode = JsonUtil.text(row, "barcode");
        r.lpnBarcode = JsonUtil.text(row, "lpnbarcode");
        r.modelEncode = JsonUtil.text(row, "modelencode");
        r.model = JsonUtil.text(row, "model");
        r.users = JsonUtil.text(row, "users");
        r.quantity = JsonUtil.asInt(row, "quantity");
        r.scanTime = parseMillis(JsonUtil.text(row, "scantime"));

        return r;
    }

    /** 由表名推导报工通道：lot -> MANUAL，barcodeautomatic -> BARCODE；未知返回 null 交给下游当脏数据处理 */
    private static String outputTypeOf(String tableName) {
        if (MesConstants.TABLE_LOT.equals(tableName)) {
            return OutputType.MANUAL.name();
        }
        if (MesConstants.TABLE_BARCODE.equals(tableName)) {
            return OutputType.BARCODE.name();
        }
        return null;
    }

    private static long parseMillis(String s) {
        if (s == null || s.isEmpty()) {
            return 0L;
        }
        try {
            if (s.contains("T")) {
                return Instant.parse(s).toEpochMilli();
            }
            return LocalDateTime.parse(s, TIMESTAMP).atZone(MesConstants.BIZ_ZONE).toInstant().toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * 解析 op_ts（TIMESTAMP_LTZ(3) 序列化的 UTC 时刻）作 version。
     * debezium-json 对 TIMESTAMP_LTZ 可能输出 {@code yyyy-MM-dd HH:mm:ss[.SSS]Z}（带 Z 不带 T），
     * 也可能输出 ISO-8601（含 T）。统一把空格换 T 交给 {@link Instant#parse}，它支持 Z/offset/毫秒。
     * 全量 snapshot 阶段 op_ts 为 epoch 0，解析出来就是 0，直接落 version=0（属预期）。
     */
    private static long parseOpTs(String s) {
        if (s == null || s.isEmpty()) {
            return 0L;
        }
        try {
            return Instant.parse(s.replace(' ', 'T')).toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }
}
