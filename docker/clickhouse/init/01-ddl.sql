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
-- 去重键 = ORDER BY，末尾必须带 record_id：
-- (company, datasource, shift_date, productionorder) 这个组合并不唯一——
-- 同一基地同一订单同一秒完全可能有多条报工（扫码通道尤其密集）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dwd_mes_report
(
    record_id         String                    COMMENT '业务唯一键：lotid 或 barcode',
    version           UInt64                    COMMENT 'CDC ts_ms，同键取最大',
    company           LowCardinality(String)    COMMENT '基地',
    datasource        LowCardinality(String)    COMMENT 'OLD_MES / NEW_MES',
    outputtype        LowCardinality(String)    COMMENT 'MES手动报工 / 过线扫码',
    productionorder   String                    COMMENT '订单编号（对齐后的键）',
    productionorderid Int64                     COMMENT '订单主键，DWS 达成率先按它聚合',
    lpnbarcode        String                    COMMENT '批次/LPN 条码',
    username          String,
    partcode          String,
    partname          String,
    linecode          LowCardinality(String)    COMMENT '产线编码',
    linename          String,
    workcentertype    LowCardinality(String),
    workshop          LowCardinality(String)    COMMENT '车间/分厂',
    orderquantity     Int64                     COMMENT '订单计划量（订单级属性，勿直接 SUM）',
    lotquantity       Decimal(38, 4)            COMMENT '报工量',
    outputdatetime    DateTime                  COMMENT '报工时间（事件时间）',
    shift_date        Date                      COMMENT '班次日，08:30 日界换算'
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY shift_date
        ORDER BY (company, datasource, shift_date, productionorder, record_id)
        SETTINGS index_granularity = 8192;

-- 下游/对账统一查这个视图，避免忘记去重
CREATE VIEW IF NOT EXISTS mes_production_monitor.v_dwd_mes_report AS
SELECT *
FROM mes_production_monitor.dwd_mes_report FINAL;

-- -----------------------------------------------------------------------------
-- DWS 分钟级产量
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mes_production_monitor.dws_report_1min
(
    ts_minute       DateTime                 COMMENT '窗口起始（分钟对齐）',
    company         LowCardinality(String),
    workshop        LowCardinality(String),
    linecode        LowCardinality(String),
    linename        String,
    partcode        String,
    output_quantity Decimal(38, 4)           COMMENT 'SUM(lotquantity)',
    record_cnt      UInt64                   COMMENT '报工笔数',
    version         UInt64                   COMMENT '窗口输出时间戳，重算时幂等覆盖'
)
    ENGINE = ReplacingMergeTree(version)
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
    shift_date        Date,
    company           LowCardinality(String),
    productionorder   String,
    productionorderid Int64,
    workshop          LowCardinality(String),
    linecode          LowCardinality(String),
    partcode          String,
    order_quantity    Int64                  COMMENT '订单计划量',
    report_quantity   Decimal(38, 4)         COMMENT '该订单累计报工量',
    achieve_rate      Decimal(10, 4)         COMMENT 'report / order',
    plan_progress     Decimal(10, 4)         COMMENT '计划进度（线性爬坡）',
    last_report_time  DateTime,
    version           UInt64
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY shift_date
        ORDER BY (company, shift_date, productionorder);

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
    company         LowCardinality(String),
    linecode        LowCardinality(String),
    productionorder String,
    order_quantity  Int64,
    report_quantity Decimal(38, 4),
    achieve_rate    Decimal(10, 4),
    alert_time      DateTime                 COMMENT '首次触发时间',
    recover_time    Nullable(DateTime)       COMMENT 'NULL = 未恢复；OVER_REPORT 不会自动恢复',
    version         UInt64
)
    ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMMDD(alert_time)
        ORDER BY (company, alert_type, alert_id);

CREATE VIEW IF NOT EXISTS mes_production_monitor.v_ads_alert AS
SELECT *
FROM mes_production_monitor.ads_alert FINAL;
