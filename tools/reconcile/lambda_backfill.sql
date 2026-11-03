-- =============================================================================
-- Lambda 批层回填：从 v_dwd_mes_report（去重后的明细）重算 DWS 聚合并覆盖。
-- 见 技术设计文档.md 6.3（DWS）+ 10.2（对账）。
--
-- 目的：流层给出低延迟结果，批层定期重算兜底——迟到没吸收的、代码 bug、sink 漏写、
-- checkpoint 回退重放造成的重复，都能靠"重算 + 覆盖"恢复。这是 Lambda 架构的批层。
--
-- 用法：定期执行（cron / 手动）。TRUNCATE + INSERT 有极短空窗，学习环境可接受；
--       生产要无缝切换，改成"写临时表 + EXCHANGE TABLES"。
--
-- 前提：先做 CK 表迁移（重建 dwd_mes_report + 两张 DWS 表，见 01-ddl.sql），
--       否则 ordercreateddate / orderduedate / datasource 等列不存在。
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. 分钟产量重算
--    SummingMergeTree 只累加不覆盖，所以必须先 TRUNCATE 再 INSERT 全量重算结果。
--    record_cnt 用 count()（明细去重后），与流层"带符号增量累加"的净笔数口径一致。
-- -----------------------------------------------------------------------------
TRUNCATE TABLE mes_production_monitor.dws_report_1min;

INSERT INTO mes_production_monitor.dws_report_1min
SELECT
    toStartOfMinute(outputdatetime) AS ts_minute,
    company,
    workshop,
    linecode,
    any(linename)                  AS linename,
    partcode,
    sum(lotquantity)               AS output_quantity,
    toInt64(count())               AS record_cnt
FROM mes_production_monitor.v_dwd_mes_report
GROUP BY ts_minute, company, workshop, linecode, partcode;


-- -----------------------------------------------------------------------------
-- 2. 订单达成率重算（含计划进度）
--    订单静态属性（orderquantity / ordercreateddate / orderduedate / 维度）用 any()——
--    它们在订单内恒定，取任意一条都一样；报工量用 sum() 聚合。
-- -----------------------------------------------------------------------------
TRUNCATE TABLE mes_production_monitor.dws_order_achievement;

INSERT INTO mes_production_monitor.dws_order_achievement
SELECT
    shift_date,
    company,
    datasource,
    productionorder,
    any(productionorderid)                                       AS productionorderid,
    any(workshop)                                                AS workshop,
    any(linecode)                                                AS linecode,
    any(partcode)                                                AS partcode,
    any(orderquantity)                                           AS order_quantity,
    sum(lotquantity)                                             AS report_quantity,
    toDecimal64(if(any(orderquantity) > 0,
                   sum(lotquantity) / any(orderquantity),
                   0), 4)                                        AS achieve_rate,
    -- 计划进度（线性爬坡）= 已用时间 / 生产周期，截断 [0,1]；now() 取批重算时刻
    -- 交期 <= 创建时间是脏数据（造数器注入过），直接给 0
    toDecimal64(if(toUnixTimestamp(any(orderduedate)) > toUnixTimestamp(any(ordercreateddate)),
                   least(1.0, greatest(0.0,
                       (toUnixTimestamp(now()) - toUnixTimestamp(any(ordercreateddate)))
                       / (toUnixTimestamp(any(orderduedate)) - toUnixTimestamp(any(ordercreateddate))))),
                   0.0), 4)                                      AS plan_progress,
    max(outputdatetime)                                          AS last_report_time
FROM mes_production_monitor.v_dwd_mes_report
GROUP BY shift_date, company, datasource, productionorder;
