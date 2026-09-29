package com.mes.production.monitor.common.constant;

import java.time.ZoneId;

/**
 * 全局业务常量。凡是「口径」相关的数字都集中在这里，不要散落在各作业里
 */
public final class MesConstants {

    // ==================== 时间口径 ====================

    /** 业务时区。事件时间统一按 +8 处理，见文档 12 节坑 #5。 */
    public static final ZoneId BIZ_ZONE = ZoneId.of("Asia/Shanghai");

    /** 班次日界：08:30。当天 08:30:00 ~ 次日 08:29:59 归属同一个班次日。 */
    public static final int SHIFT_START_HOUR = 8;
    public static final int SHIFT_START_MINUTE = 30;

    /**
     * Flink 窗口 offset 的正确值（毫秒）。
     * <p>Flink 窗口边界基于 UTC epoch 计算，08:30(+8) 对应 UTC 00:30，
     * 所以 offset = 8.5h - 8h = 0.5h，而不是 8.5h。写成 8.5h 会导致班次日整体偏移 8 小时，
     * 且错得极其隐蔽——数据一条不少、总量也对，只有归属日期错了。见文档 6.3.1。
     * <p>本项目不依赖窗口 offset，而是显式计算 shift_date；此常量保留作对照与单测断言用。
     */
    public static final long SHIFT_WINDOW_OFFSET_MS = 30 * 60 * 1000L;

    // ==================== 过滤口径（沿用原报表，见文档 2.4） ====================

    /** 订单号前缀黑名单：NOT LIKE 'FG%' */
    public static final String EXCLUDED_ORDER_PREFIX = "FG";

    // ==================== 库名解析（见文档 2.1） ====================

    public static final String DB_PREFIX_OLD = "mes_old_";
    public static final String DB_PREFIX_NEW = "mes_new_";

    // ==================== 源表名 ====================
    // CDC 作业 CdcToKafkaJob 的 SQL DDL 复用这些常量，改源表名只需改这里一处

    public static final String TABLE_LOT = "lot";
    public static final String TABLE_BARCODE = "barcodeautomatic";
    public static final String TABLE_PRODUCTION_ORDER = "productionorder";
    public static final String TABLE_PART = "part";
    public static final String TABLE_WORKCENTER = "workcenter";
    public static final String TABLE_WORKSITE = "worksite";
    public static final String TABLE_USER = "user";

    private MesConstants() {
    }
}
