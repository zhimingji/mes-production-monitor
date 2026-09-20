-- =============================================================================
-- 对账结果：查 ClickHouse。与 mysql_baseline.sql 逐粒度比对。见文档 10.2
--
-- 【必须走 v_ 视图或加 FINAL】
-- ReplacingMergeTree 只在后台 merge 时去重，merge 时机不确定。
-- 直接查基表会看到未合并的重复行，对账结果必然偏大——
-- 这是第一次对账最常见的"假差异"，别在业务逻辑里白找半天。
--
-- {{FROM}} / {{TO}} 与基线保持一致
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 粒度 1：基地 × 班次日 × 通道
--
-- 与基线比对时注意：扫码通道的笔数必然不一致。
-- 基线按 LPN 分组（原报表口径），这里是明细粒度（DWD 层刻意不聚合，见文档 6.2.2）。
-- report_qty 应该一致，record_cnt 只在 MANUAL 通道可比。
-- -----------------------------------------------------------------------------
SELECT company,
       shift_date,
       outputtype,
       sum(lotquantity) AS report_qty,
       count()          AS record_cnt
FROM mes_production_monitor.v_dwd_mes_report
WHERE outputdatetime >= toDateTime('{{FROM}}')
  AND outputdatetime < toDateTime('{{TO}}')
GROUP BY company, shift_date, outputtype
ORDER BY company, shift_date;


-- -----------------------------------------------------------------------------
-- 粒度 2：车间 × 产线
-- -----------------------------------------------------------------------------
SELECT company,
       workshop,
       linecode,
       sum(lotquantity) AS report_qty,
       count()          AS record_cnt
FROM mes_production_monitor.v_dwd_mes_report
WHERE outputdatetime >= toDateTime('{{FROM}}')
  AND outputdatetime < toDateTime('{{TO}}')
GROUP BY company, workshop, linecode
ORDER BY report_qty DESC;


-- -----------------------------------------------------------------------------
-- 粒度 3：订单级达成率
--
-- 这里演示了 7.2 的正确口径：先按订单聚合报工量，再关联订单量。
-- orderquantity 用 any() 而不是 sum()——它是订单级属性，
-- sum 的话同一订单有几笔报工就会重复累加几次。
-- -----------------------------------------------------------------------------
SELECT company,
       productionorder,
       any(orderquantity)                                     AS order_qty,
       sum(lotquantity)                                       AS report_qty,
       sum(lotquantity) / nullIf(any(orderquantity), 0)       AS achieve_rate
FROM mes_production_monitor.v_dwd_mes_report
WHERE outputdatetime >= toDateTime('{{FROM}}')
  AND outputdatetime < toDateTime('{{TO}}')
GROUP BY company, productionorder
ORDER BY achieve_rate DESC;


-- -----------------------------------------------------------------------------
-- 粒度 4：维度级达成率（验证 DWS 订单表的口径）
--
-- 关键：这里可以直接 SUM(order_quantity)，因为 dws_order_achievement 已经是订单粒度
-- （一个订单一行），维度聚合不会重复累加。这就是 7.2 说的"先订单聚合再维度聚合"。
-- -----------------------------------------------------------------------------
SELECT company,
       workshop,
       linecode,
       sum(report_quantity)                                   AS report_qty,
       sum(order_quantity)                                    AS order_qty,
       sum(report_quantity) / nullIf(sum(order_quantity), 0)   AS achieve_rate
FROM mes_production_monitor.v_dws_order_achievement
WHERE shift_date >= toDate('{{FROM}}')
  AND shift_date <= toDate('{{TO}}')
GROUP BY company, workshop, linecode
ORDER BY achieve_rate;


-- -----------------------------------------------------------------------------
-- 自查：去重是否生效
--
-- 如果这个查询有结果，说明有 record_id 存在多个版本还没 merge。
-- 这不是数据错误（视图会去重），但它解释了"为什么不加 FINAL 查出来的数偏大"。
-- 也可以用它验证报工修正确实产生了多版本。
-- -----------------------------------------------------------------------------
SELECT record_id,
       count()          AS versions,
       min(version)     AS first_version,
       max(version)     AS last_version
FROM mes_production_monitor.dwd_mes_report
WHERE shift_date >= toDate('{{FROM}}')
GROUP BY record_id
HAVING versions > 1
ORDER BY versions DESC
LIMIT 50;
