-- =============================================================================
-- ClickHouse 表结构。见 技术设计文档.md 5.3
--
-- 三张表全部用 ReplacingMergeTree：
--   1) lot 会被 UPDATE 修正报工量，必须能按业务唯一键取最新版本；
--   2) Flink sink 是 at-least-once，作业重启会重放，必须靠幂等覆盖而不是靠 sink 语义。
--
-- 【必须知道】ReplacingMergeTree 只在后台 merge 时去重，merge 时机不确定。
-- 查询侧不加 FINAL 或不自己 argMax/GROUP BY，就会看到重复行。
-- 每张表后面都提供了去重视图，下游和对账脚本一律走视图。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS mes_production_monitor;

-- -----------------------------------------------------------------------------
-- DWD 明细宽表
-- 去重键 = ORDER BY，只放稳定字段，末尾 record_id 保证唯一：
--   (company, datasource, shift_date) 本身不唯一——同一基地同一秒完全可能有多条报工
--   （扫码通道尤其密集），必须落到 record_id 才唯一。
--   productionorder 不进键：它是 join 出来的可变维度，报工改挂订单 / 订单号纠错会
--   让它变，导致同一条报工裂成两个键、不去重，产量翻倍（见文档 6.1.1）。
--   outputtype 兜底 record_id 跨通道撞键（lotid 与 barcode 可能同串）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dwd_mes_report
(
    record_id         String                    COMMENT '业务唯一键：lotid 或 barcode',
    version           UInt64                    COMMENT 'CDC 操作时间戳 op_ts，同键取最大',
    company           LowCardinality(String)    COMMENT '基地',
    datasource        LowCardinality(String)    COMMENT 'OLD_MES / NEW_MES',
    outputtype        LowCardinality(String)    COMMENT '手动报工 / 过线扫码',
    productionorder   String                    COMMENT '订单编号（两通道对齐到这同一列，非去重键）',
    productionorderid Int64                     COMMENT '订单主键，DWS 达成率先按它聚合',
    lpnbarcode        String                    COMMENT '批次/LPN 条码',
    username          String                    COMMENT '报工人',
    partcode          String                    COMMENT '物料编码',
    partname          String                    COMMENT '物料名称',
    linecode          LowCardinality(String)    COMMENT '产线编码',
    linename          String                    COMMENT '产线名称',
    workcentertype    LowCardinality(String)    COMMENT '产线类型',
    workshop          LowCardinality(String)    COMMENT '车间/分厂',
    orderquantity     Int64                     COMMENT '订单计划量（订单级属性，勿直接 SUM）',
    ordercreateddate  DateTime                  COMMENT '订单创建时间（计划进度爬坡起点）',
    orderduedate      DateTime                  COMMENT '订单交期（计划进度爬坡终点）',
    lotquantity       Decimal(38, 4)            COMMENT '报工量',
    outputdatetime    DateTime                  COMMENT '报工时间（事件时间）',
    shift_date        Date                      COMMENT '班次日，08:30 日界换算',
    is_deleted        UInt8 DEFAULT 0           COMMENT '物理删除墓碑：1=已删除，查询侧过滤'
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY shift_date
        ORDER BY (company, datasource, shift_date, outputtype, record_id)
        SETTINGS index_granularity = 8192;

-- 下游/对账统一查这个视图，避免忘记去重
-- is_deleted = 0：把物理删除的墓碑行也一并滤掉，见文档 6.1.1
CREATE VIEW IF NOT EXISTS mes_production_monitor.v_dwd_mes_report AS
SELECT *
FROM mes_production_monitor.dwd_mes_report FINAL
WHERE is_deleted = 0;

-- -----------------------------------------------------------------------------
-- 报工物理删除审计（op=d 的完整 before 留证）
--
-- append-only MergeTree，不走 ReplacingMergeTree 去重：
-- 审计要的是"原样留证"，at-least-once 重放重复写几笔可接受。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dwd_mes_report_deleted_audit
(
    record_id   String                     COMMENT '被删报工的业务唯一键',
    company     LowCardinality(String)    COMMENT '基地',
    table_name  String                     COMMENT 'lot / barcodeautomatic',
    ts_ms       UInt64                     COMMENT '删除事件 ts_ms',
    payload     String                     COMMENT '完整 before JSON',
    audit_time  DateTime                   COMMENT '入库时间'
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(audit_time)
        ORDER BY (company, audit_time, record_id);

-- -----------------------------------------------------------------------------
-- DWS 分钟级产量
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dws_report_1min
(
    ts_minute       DateTime                 COMMENT '窗口起始（分钟对齐）',
    company         LowCardinality(String)    COMMENT '基地',
    workshop        LowCardinality(String)    COMMENT '车间/分厂',
    linecode        LowCardinality(String)    COMMENT '产线编码',
    linename        String                    COMMENT '产线名称',
    partcode        String                    COMMENT '物料编码',
    output_quantity Decimal(38, 4)           COMMENT 'SUM(lotquantity)，带符号增量累加',
    record_cnt      Int64                    COMMENT '报工笔数，带符号增量累加（修正/删除中间态可为负，merge 后非负）'
)
    ENGINE = SummingMergeTree((output_quantity, record_cnt))
        PARTITION BY toYYYYMMDD(ts_minute)
        ORDER BY (company, ts_minute, workshop, linecode, partcode);

CREATE VIEW IF NOT EXISTS mes_production_monitor.v_dws_report_1min AS
SELECT *
FROM mes_production_monitor.dws_report_1min FINAL;

-- -----------------------------------------------------------------------------
-- DWS 订单级达成率
--
-- 达成率只落到订单粒度，维度级达成率由查询侧现算：
--   SELECT company, linecode,
--          SUM(report_quantity) / SUM(order_quantity) AS achieve_rate
--   FROM mes_production_monitor.v_dws_order_achievement
--   WHERE shift_date = today()
--   GROUP BY company, linecode;
--
-- 这张表已经是订单去重的，所以维度 SUM 不会重复累加 order_quantity（见文档 7.2）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dws_order_achievement
(
    shift_date        Date                    COMMENT '班次日（08:30 日界）',
    company           LowCardinality(String) COMMENT '基地',
    datasource        LowCardinality(String) COMMENT 'OLD_MES / NEW_MES：订单号在 old/new 库同名，靠它消歧',
    productionorder   String                  COMMENT '订单编号',
    productionorderid Int64                   COMMENT '订单主键',
    workshop          LowCardinality(String) COMMENT '车间/分厂',
    linecode          LowCardinality(String) COMMENT '产线编码',
    partcode          String                  COMMENT '物料编码',
    order_quantity    Int64                  COMMENT '订单计划量',
    report_quantity   Decimal(38, 4)         COMMENT '该订单累计报工量',
    achieve_rate      Decimal(10, 4)         COMMENT 'report / order',
    plan_progress     Decimal(10, 4)         COMMENT '计划进度（线性爬坡）',
    last_report_time  DateTime               COMMENT '该订单最后一次报工时间',
    version           UInt64                 COMMENT '版本号（快照输出时间戳，幂等覆盖）'
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY shift_date
        ORDER BY (company, datasource, shift_date, productionorder);

CREATE VIEW IF NOT EXISTS mes_production_monitor.v_dws_order_achievement AS
SELECT *
FROM mes_production_monitor.dws_order_achievement FINAL;

-- -----------------------------------------------------------------------------
-- ADS 告警
-- 升级时保持 alert_id 不变、更新 level 与 version，靠 ReplacingMergeTree 覆盖，
-- 这样大屏看到的是一条告警的当前状态，而不是 P3/P2/P1 三条重复记录。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.ads_alert
(
    alert_id        String                   COMMENT '告警唯一标识，升级时不变',
    alert_type      LowCardinality(String)   COMMENT 'OVER_REPORT / LAG / MISSING_REPORT',
    level           LowCardinality(String)   COMMENT 'P3 / P2 / P1',
    company         LowCardinality(String)   COMMENT '基地',
    linecode        LowCardinality(String)   COMMENT '产线编码',
    productionorder String                   COMMENT '订单编号',
    order_quantity  Int64                    COMMENT '订单计划量',
    report_quantity Decimal(38, 4)           COMMENT '累计报工量',
    achieve_rate    Decimal(10, 4)           COMMENT '达成率',
    alert_time      DateTime                 COMMENT '首次触发时间',
    recover_time    Nullable(DateTime)       COMMENT 'NULL = 未恢复；OVER_REPORT 不会自动恢复',
    version         UInt64                   COMMENT '版本号（升级时更新，ReplacingMergeTree 覆盖）'
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMMDD(alert_time)
        ORDER BY (company, alert_type, alert_id);

CREATE VIEW IF NOT EXISTS mes_production_monitor.v_ads_alert AS
SELECT *
FROM mes_production_monitor.ads_alert FINAL;
