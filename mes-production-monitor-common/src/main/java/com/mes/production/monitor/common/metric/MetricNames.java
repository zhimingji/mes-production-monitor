package com.mes.production.monitor.common.metric;

/**
 * 自定义 metric 名称。见文档 11.3。
 *
 * <p>数据链路最危险的不是报错，是<b>静默丢数据</b>——被过滤的、维度 join 不上的、迟到丢弃的，
 * 如果只是默默 drop，出问题时无从定位。所有丢弃/降级路径都必须有计数。
 *
 * <p>统一挂在 metric group {@link #GROUP} 下，Flink Web UI 里可直接查看。
 */
public final class MetricNames {

    public static final String GROUP = "mes-production-monitor";

    /** 解析失败 / 关键字段缺失的脏数据 */
    public static final String DIRTY_RECORD_COUNT = "dirty_record_count";

    /** 被业务规则过滤掉的记录（qty <= 0、订单号 FG% 开头） */
    public static final String FILTERED_RECORD_COUNT = "filtered_record_count";

    /**
     * 维表 join 不上的记录数。建议按维表分标签：order / part / workcenter / worksite / user。
     * <p>维度 miss 是典型的静默失败：条数一条不少，只是维度字段全空。
     * 广播维表初始化竞态（文档 6.2.1）就靠这个指标发现。
     */
    public static final String DIM_MISS_COUNT = "dim_miss_count";

    /** 超出 watermark 容忍度的迟到数据（走 side output 补数的那些） */
    public static final String LATE_RECORD_COUNT = "late_record_count";

    /** 告警触发数，按 type + level 分标签 */
    public static final String ALERT_FIRED_COUNT = "alert_fired_count";

    /** ClickHouse 攒批 flush 耗时（毫秒），用于定位写入瓶颈 */
    public static final String CK_FLUSH_DURATION_MS = "ck_flush_duration_ms";

    /** ClickHouse 累计写入行数 */
    public static final String CK_WRITTEN_ROWS = "ck_written_rows";

    /** 告警作业状态大小（MB），配合状态 TTL 观察是否膨胀，见文档 6.4.1 */
    public static final String STATE_SIZE_MB = "state_size_mb";

    private MetricNames() {
    }
}
