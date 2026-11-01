package com.mes.production.monitor.dwd.dim;

import com.mes.production.monitor.common.model.DimTables;

import java.io.Serializable;

/**
 * 维表广播流元素。一个广播流要同时承载两类更新，所以用一个带类型标签的 POJO 区分：
 *
 * <ul>
 *   <li>{@link Kind#STATIC_SNAPSHOT}：3 张静态维表（workcenter/worksite/user）的定时全量快照，
 *       整份替换对应字段（part 走 {@link PartLookup} 按需查询，不进快照）；</li>
 *   <li>{@link Kind#ORDER_UPSERT} / {@link Kind#ORDER_DELETE}：订单维的 CDC 增量，按 id 增删。</li>
 * </ul>
 *
 * <p>订单维单独走 CDC 而不是和静态维表一起定时全量刷，原因见文档 6.2.4：它会 UPDATE，
 * 订单量/交期的修正直接影响达成率与告警，定时全量会漏掉变更窗口。
 *
 * <p>为什么用一个类而不是两个 source 各自广播：{@code BroadcastProcessFunction} 只支持一个广播流，
 * 所以两个 source 先 {@code union} 成一个 {@code DataStream<DimUpdate>} 再广播，下游按 {@code kind} 分发。
 */
public class DimUpdate implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Kind { STATIC_SNAPSHOT, ORDER_UPSERT, ORDER_DELETE }

    public Kind kind;

    /** STATIC_SNAPSHOT：3 张静态维表的全量快照（orders 字段为空，不参与；part 走 PartLookup） */
    public DimCache cache;

    /** ORDER_UPSERT / ORDER_DELETE：源库名（如 mes_old_zh），订单 id 只在库内唯一 */
    public String db;
    public Long orderId;

    /** ORDER_UPSERT：新订单记录；ORDER_DELETE 时为 null */
    public DimTables.ProductionOrder order;

    public DimUpdate() {
    }

    private DimUpdate(Kind kind) {
        this.kind = kind;
    }

    public static DimUpdate staticSnapshot(DimCache cache) {
        DimUpdate u = new DimUpdate(Kind.STATIC_SNAPSHOT);
        u.cache = cache;
        return u;
    }

    public static DimUpdate orderUpsert(String db, long orderId, DimTables.ProductionOrder order) {
        DimUpdate u = new DimUpdate(Kind.ORDER_UPSERT);
        u.db = db;
        u.orderId = orderId;
        u.order = order;
        return u;
    }

    public static DimUpdate orderDelete(String db, long orderId) {
        DimUpdate u = new DimUpdate(Kind.ORDER_DELETE);
        u.db = db;
        u.orderId = orderId;
        return u;
    }
}
