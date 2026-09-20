#!/bin/bash
# ==============================================================================
# 初始化 10 个 MES 库（5 基地 × 新旧 MES），见文档 2.1
#
#   mes_old_zh  mes_new_zh
#   mes_old_zz  mes_new_zz
#   mes_old_hf  mes_new_hf
#   mes_old_wh  mes_new_wh
#   mes_old_cq  mes_new_cq
#
# 差异：旧 MES 有 lot + it_barcodeautomatic 两套报工通道，新 MES 只有 lot（见文档 2.2）
# ==============================================================================
set -euo pipefail

COMPANIES="zh zz hf wh cq"
MYSQL="mysql -uroot -p${MYSQL_ROOT_PASSWORD} --default-character-set=utf8mb4"

echo "[mes-production-monitor] 创建账号..."
$MYSQL <<'SQL'
-- CDC 专用账号：需要 REPLICATION 权限才能读 binlog（见文档 12 节坑 #2）
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED WITH mysql_native_password BY 'cdc123456';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';

-- 应用账号：造数器和维表 bootstrap 用
CREATE USER IF NOT EXISTS 'mes_monitor'@'%' IDENTIFIED WITH mysql_native_password BY 'mesMonitor123456';
GRANT ALL PRIVILEGES ON `mes_old_%`.* TO 'mes_monitor'@'%';
GRANT ALL PRIVILEGES ON `mes_new_%`.* TO 'mes_monitor'@'%';

FLUSH PRIVILEGES;
SQL

# ------------------------------------------------------------------------------
# 公共 schema：新旧 MES 都有
# ------------------------------------------------------------------------------
common_schema() {
cat <<'SQL'
-- ============ 维表 ============

CREATE TABLE IF NOT EXISTS worksite (
  worksiteid   BIGINT       NOT NULL COMMENT '车间/分厂 ID',
  description  VARCHAR(100) NOT NULL COMMENT '如 涡旋分厂/总装分厂/泵体分厂',
  PRIMARY KEY (worksiteid)
) ENGINE=InnoDB COMMENT='车间/分厂';

CREATE TABLE IF NOT EXISTS workcenter (
  workcenterid   BIGINT       NOT NULL COMMENT '工作中心（产线）ID',
  workcentername VARCHAR(50)  NOT NULL COMMENT '产线编码，如 D32Z01',
  description    VARCHAR(100) NULL     COMMENT '产线名称，如 总装A线',
  worksiteid     BIGINT       NOT NULL COMMENT '所属车间',
  workcentertype VARCHAR(50)  NULL     COMMENT '产线类型，如 总装',
  PRIMARY KEY (workcenterid),
  KEY idx_worksite (worksiteid)
) ENGINE=InnoDB COMMENT='工作中心/产线';

CREATE TABLE IF NOT EXISTS part (
  partid   BIGINT       NOT NULL COMMENT '物料 ID',
  partcode VARCHAR(50)  NOT NULL COMMENT '物料编码',
  partname VARCHAR(200) NULL     COMMENT '物料名称',
  PRIMARY KEY (partid),
  KEY idx_partcode (partcode)
) ENGINE=InnoDB COMMENT='物料';

CREATE TABLE IF NOT EXISTS `user` (
  userid   BIGINT      NOT NULL COMMENT '用户 ID',
  username VARCHAR(50) NOT NULL COMMENT '用户名',
  PRIMARY KEY (userid)
) ENGINE=InnoDB COMMENT='用户';

-- ============ 生产订单 ============

CREATE TABLE IF NOT EXISTS productionorder (
  productionorderid BIGINT      NOT NULL COMMENT '订单主键',
  productionorderno VARCHAR(50) NOT NULL COMMENT '订单编号；DWD 层统一用它做键',
  orderquantity     BIGINT      NOT NULL DEFAULT 0 COMMENT '订单数量（计划数）',
  worksiteid        BIGINT      NULL COMMENT '车间/仓库 ID',
  partid            BIGINT      NULL COMMENT '物料 ID',
  createddate       DATETIME    NULL COMMENT '创建时间；计划爬坡的起点（文档 7.3）',
  duedate           DATETIME    NULL COMMENT '交期；计划爬坡的终点 + 滞后预警依据',
  PRIMARY KEY (productionorderid),
  UNIQUE KEY uk_orderno (productionorderno),
  KEY idx_createddate (createddate)
) ENGINE=InnoDB COMMENT='生产订单';

-- ============ 手动报工 ============

CREATE TABLE IF NOT EXISTS lot (
  lotid             BIGINT        NOT NULL COMMENT '批次主键；DWD 的 record_id 来源',
  lotno             VARCHAR(50)   NOT NULL COMMENT '批次号',
  productionorderid BIGINT        NOT NULL COMMENT '生产订单 ID（数字键）',
  workcenterid      BIGINT        NULL     COMMENT '报工所在产线；本地建模新增，用于关联 workcenter',
  finishedquantity  DECIMAL(38,4) NOT NULL DEFAULT 0 COMMENT '完成量；会被 UPDATE 修正',
  createddate       DATETIME      NULL     COMMENT '报工时间（事件时间）',
  createdbyid       BIGINT        NULL     COMMENT '报工人 ID',
  PRIMARY KEY (lotid),
  KEY idx_order (productionorderid),
  KEY idx_createddate (createddate)
) ENGINE=InnoDB COMMENT='批次/手动报工';
SQL
}

# ------------------------------------------------------------------------------
# 仅旧 MES：过线扫码
# ------------------------------------------------------------------------------
old_only_schema() {
cat <<'SQL'
CREATE TABLE IF NOT EXISTS it_barcodeautomatic (
  barcode          VARCHAR(50)  NOT NULL COMMENT '条码；DWD 的 record_id 来源',
  lpnbarcode       VARCHAR(50)  NULL     COMMENT 'LPN 条码；原报表按它分组聚合',
  productionorderid BIGINT      NOT NULL COMMENT '生产订单 ID；与 lot 使用相同订单键',
  modelencod       VARCHAR(50)  NULL     COMMENT '型号编码，对应 partcode',
  model            VARCHAR(100) NULL     COMMENT '型号，对应 partname',
  users            VARCHAR(50)  NULL     COMMENT '报工用户名（字符串，非 ID）',
  quantity         INT          NULL     COMMENT '数量；原报表按 LPN 分组 SUM',
  scantime         DATETIME     NULL     COMMENT '扫描时间（事件时间）',
  PRIMARY KEY (barcode),
  KEY idx_order (productionorderid),
  KEY idx_scantime (scantime)
) ENGINE=InnoDB COMMENT='过线扫码（仅旧 MES）';
SQL
}

for company in $COMPANIES; do
  for prefix in mes_old mes_new; do
    db="${prefix}_${company}"
    echo "[mes-production-monitor] 初始化 ${db} ..."
    $MYSQL -e "CREATE DATABASE IF NOT EXISTS \`${db}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    common_schema | $MYSQL "${db}"
    if [ "${prefix}" = "mes_old" ]; then
      old_only_schema | $MYSQL "${db}"
    fi
  done
done

echo "[mes-production-monitor] 10 个 MES 库初始化完成。库清单："
$MYSQL -e "SHOW DATABASES LIKE 'mes\_%';"
