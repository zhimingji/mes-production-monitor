-- =============================================================================
-- 对账基线：用【原报表口径】直接查 MySQL。见 技术设计文档.md 10.2
--
-- 这是唯一能证明"实时链路算对了"的东西。跑通 ≠ 算对。
--
-- 用法：把 {{DB}} 替换成库名逐库执行，或用脚本循环 10 个库后汇总。
--       {{FROM}} / {{TO}} 是对账时间范围（报工时间），格式 '2026-09-10 08:30:00'
--
-- 口径必须与原报表严格一致（文档 2.4）：
--   1. 班次日界 08:30  -> DATE_SUB(时间, INTERVAL 510 MINUTE) 后取 DATE
--      510 = 8*60 + 30
--   2. finishedquantity > 0
--   3. 订单号 NOT LIKE 'FG%'
--   4. 扫码通道按 (lpnbarcode, productionorderid, modelencode, model, users) 分组，
--      SUM(quantity) 取报工量、MAX(scantime) 取报工时间
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 粒度 1：基地 × 班次日
-- -----------------------------------------------------------------------------

-- 手动报工通道（新旧 MES 都有）
SELECT '{{DB}}'                                                  AS db_name,
       DATE(DATE_SUB(l.createddate, INTERVAL 510 MINUTE))        AS shift_date,
       'MANUAL'                                                  AS channel,
       SUM(l.finishedquantity)                                   AS report_qty,
       COUNT(*)                                                  AS record_cnt
FROM lot l
         JOIN productionorder po ON po.productionorderid = l.productionorderid
WHERE l.finishedquantity > 0
  AND po.productionorderno NOT LIKE 'FG%'
  AND l.createddate >= '{{FROM}}'
  AND l.createddate < '{{TO}}'
GROUP BY 1, 2, 3
ORDER BY 2;


-- 过线扫码通道（仅旧 MES；对 mes_new_* 库跳过这段）
--
-- 注意内层先按 LPN 分组：这是原报表的口径。
-- 【重要】因此本查询的 record_cnt 是"LPN 数"，而 ClickHouse 侧 DWD 是明细粒度，
-- record_cnt 必然不同，这不是 bug。对账要比的是 report_qty，
-- 笔数只在手动报工通道有可比性。
SELECT '{{DB}}'                                            AS db_name,
       DATE(DATE_SUB(t.scan_time, INTERVAL 510 MINUTE))    AS shift_date,
       'BARCODE'                                           AS channel,
       SUM(t.qty)                                          AS report_qty,
       COUNT(*)                                            AS lpn_cnt
FROM (SELECT lpnbarcode,
             productionorderid,
             modelencode,
             model,
             users,
             SUM(quantity)  AS qty,
             MAX(scantime)   AS scan_time
      FROM barcodeautomatic
      WHERE scantime >= '{{FROM}}'
        AND scantime < '{{TO}}'
      GROUP BY lpnbarcode, productionorderid, modelencode, model, users) t
         JOIN productionorder po ON po.productionorderid = t.productionorderid
WHERE t.qty > 0
  AND po.productionorderno NOT LIKE 'FG%'
GROUP BY 1, 2, 3
ORDER BY 2;


-- -----------------------------------------------------------------------------
-- 粒度 2：车间 × 产线
-- -----------------------------------------------------------------------------
SELECT '{{DB}}'                                            AS db_name,
       ws.description                                      AS workshop,
       wc.workcentername                                   AS linecode,
       SUM(l.finishedquantity)                             AS report_qty,
       COUNT(*)                                            AS record_cnt
FROM lot l
         JOIN productionorder po ON po.productionorderid = l.productionorderid
         LEFT JOIN workcenter wc ON wc.workcenterid = l.workcenterid
         LEFT JOIN worksite ws ON ws.worksiteid = wc.worksiteid
WHERE l.finishedquantity > 0
  AND po.productionorderno NOT LIKE 'FG%'
  AND l.createddate >= '{{FROM}}'
  AND l.createddate < '{{TO}}'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;


-- -----------------------------------------------------------------------------
-- 粒度 3：订单级（达成率对账，也是排查差异时最容易定位到具体记录的粒度）
-- -----------------------------------------------------------------------------
SELECT '{{DB}}'                                            AS db_name,
       po.productionorderno                                AS productionorder,
       po.orderquantity                                    AS order_qty,
       SUM(l.finishedquantity)                             AS report_qty,
       SUM(l.finishedquantity) / NULLIF(po.orderquantity, 0) AS achieve_rate
FROM lot l
         JOIN productionorder po ON po.productionorderid = l.productionorderid
WHERE l.finishedquantity > 0
  AND po.productionorderno NOT LIKE 'FG%'
  AND l.createddate >= '{{FROM}}'
  AND l.createddate < '{{TO}}'
GROUP BY 1, 2, 3
ORDER BY 5 DESC;
