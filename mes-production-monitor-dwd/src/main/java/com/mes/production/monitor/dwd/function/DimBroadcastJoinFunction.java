package com.mes.production.monitor.dwd.function;

import com.mes.production.monitor.common.constant.MesConstants;
import com.mes.production.monitor.common.enums.OutputType;
import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.DimTables;
import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.OdsReportRecord;
import com.mes.production.monitor.common.util.ShiftUtil;
import com.mes.production.monitor.dwd.dim.DimCache;
import com.mes.production.monitor.dwd.dim.DimLoader;
import com.mes.production.monitor.dwd.dim.DimUpdate;
import com.mes.production.monitor.dwd.dim.PartLookup;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * DWD 核心算子：维表广播 join + 订单关联 + 口径过滤 + 双通道统一。见文档 6.2
 *
 * <h3>广播初始化竞态</h3>
 * {@code open()} 里先用 {@link DimLoader} 做一次 JDBC 全量 bootstrap，
 * 因此第一条数据到来时本地缓存一定是就绪的，不存在"广播还没到导致维度全空"的窗口。
 * 广播流只负责后续增量刷新。见文档 6.2.1。
 *
 * <h3>两类广播更新</h3>
 * 3 张静态维表（workcenter/worksite/user）走定时全量快照；订单维（productionorder）会 UPDATE，
 * 走 {@code ods-mes-order} 的 CDC 增量，见文档 6.2.4。两者 union 成同一个广播流，用 {@link DimUpdate#kind} 区分。
 * 物料（part）不广播：20 万/基地的大维表，由 {@link PartLookup} 按需查询。
 *
 * <h3>维度 miss 计数</h3>
 * 维度 join 不上是<b>静默失败</b>的典型——数据条数一条不少，只是维度字段全空。
 * 各维表分别有 Counter，Flink Web UI 里能直接看到。
 *
 * <h3>核心逻辑：{@link #processElement}</h3>
 * 这是本项目业务语义最集中的一个方法，四件事：
 * <ol>
 *   <li><b>changelog 处理</b>：根据 {@code op} 决定这条记录怎么落。见文档 6.1.1。</li>
 *   <li><b>订单关联</b>：两个通道都用 productionorderid 关联订单维表，
 *       取得统一的 productionorderno。见文档 2.4 第 4 条。</li>
 *   <li><b>维表关联</b>：order / part / workcenter / worksite / user，每个 miss 都要计数。</li>
 *   <li><b>口径过滤</b>：{@code lotquantity > 0}、订单号 {@code NOT LIKE 'FG%'}，
 *       过滤掉的要计入 {@code filtered_record_count}。见文档 2.4。</li>
 * </ol>
 *
 * <p><b>注意：本层不做聚合。</b>扫码通道的 LPN 聚合边界在流上没有天然答案，
 * 保持明细、把边界交给 DWS 的窗口显式定义。见文档 6.2.2。
 */
public class DimBroadcastJoinFunction
        extends BroadcastProcessFunction<OdsReportRecord, DimUpdate, DwdReportRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DimBroadcastJoinFunction.class);

    /** 广播状态描述符。作业侧 {@code stream.broadcast(DESCRIPTOR)} 要用同一个实例 */
    public static final MapStateDescriptor<String, DimCache> DIM_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("dim-cache", String.class, DimCache.class);

    /** 物理删除审计侧流标签。op=d 的完整 before 从这里旁路出去，落 CK 审计表，见文档 6.1.1 */
    public static final OutputTag<OdsReportRecord> DELETED_AUDIT_TAG =
            new OutputTag<OdsReportRecord>("deleted-report-audit") {};

    private static final String STATE_KEY = "ALL";

    private final String jdbcUrlTemplate;
    private final String username;
    private final String password;
    private final int partCacheMaxSize;

    /** 本地缓存：open() bootstrap 得到，广播到达后被替换 */
    private transient DimCache localCache;

    /** part 大维表按需查询缓存（不进广播态，见 {@link PartLookup}） */
    private transient PartLookup partLookup;

    private transient Counter dirtyCount;
    private transient Counter filteredCount;
    private transient Counter deletedCount;
    private transient Counter orderMissCount;
    private transient Counter partMissCount;
    private transient Counter workCenterMissCount;
    private transient Counter workSiteMissCount;
    private transient Counter userMissCount;

    public DimBroadcastJoinFunction(String jdbcUrlTemplate, String username, String password, int partCacheMaxSize) {
        this.jdbcUrlTemplate = jdbcUrlTemplate;
        this.username = username;
        this.password = password;
        this.partCacheMaxSize = partCacheMaxSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 关键：bootstrap 兜底，消除广播竞态窗口（文档 6.2.1）
        this.localCache = DimLoader.loadAll(jdbcUrlTemplate, username, password);
        LOG.info("subtask {} 维表 bootstrap 完成: {}",
                getRuntimeContext().getIndexOfThisSubtask(), localCache.summary());

        // part 大维表：按需查询缓存，冷启动第一波命中前会各查一次 MySQL
        this.partLookup = new PartLookup(jdbcUrlTemplate, username, password, partCacheMaxSize);
        this.partLookup.open();

        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
        this.dirtyCount = g.counter(MetricNames.DIRTY_RECORD_COUNT);
        this.filteredCount = g.counter(MetricNames.FILTERED_RECORD_COUNT);
        this.deletedCount = g.counter(MetricNames.DELETED_RECORD_COUNT);
        MetricGroup miss = g.addGroup(MetricNames.DIM_MISS_COUNT);
        this.orderMissCount = miss.counter("order");
        this.partMissCount = miss.counter("part");
        this.workCenterMissCount = miss.counter("workcenter");
        this.workSiteMissCount = miss.counter("worksite");
        this.userMissCount = miss.counter("user");
    }

    @Override
    public void processElement(OdsReportRecord value,
                              ReadOnlyContext ctx,
                              Collector<DwdReportRecord> out) throws Exception {
        // 1) changelog：c/r/u 都按"最新值"下发；d 是物理删除，产出 is_deleted=1 的墓碑
        //    （同 recordId + 更高 version，CK ReplacingMergeTree 覆盖原行，查询侧过滤），
        //    而不是静默丢弃。parser 已把 d 路由到 before 行，所以下面 join/过滤走的是删除前原值。
        boolean isDeleted = "d".equals(value.op);
        if (isDeleted) {
            deletedCount.inc();
            // 审计侧流：在任何过滤之前把完整 before 落审计，物理删除可对账、可事后重放。
            ctx.output(DELETED_AUDIT_TAG, value);
        }

        // 通道路由：outputType 由 ReportCdcParser 按表名推导，缺失说明源表异常
        if (value.outputType == null) {
            dirtyCount.inc();
            return;
        }
        boolean barcode = OutputType.BARCODE.name().equals(value.outputType);

        // 2) 口径过滤一：报工量必须 > 0。扫码通道本层不聚合（文档 6.2.2），quantity 即单条明细量
        BigDecimal lotQuantity = barcode
                ? (value.quantity == null ? null : BigDecimal.valueOf(value.quantity))
                : value.finishedQuantity;
        if (lotQuantity == null || lotQuantity.signum() <= 0) {
            filteredCount.inc();
            return;
        }

        // 3) 订单关联：两通道都用 productionorderid 查订单维，取统一 productionorderno。
        //    查不到 -> 留空输出 + 计数，不丢（对账时数量对不上还能定位是订单没关联上）
        DimTables.ProductionOrder order = localCache.order(value.databaseName, value.productionOrderId);
        String productionOrderNo = null;
        long orderQuantity = 0L;
        long orderCreatedDate = 0L;
        long orderDueDate = 0L;
        if (order == null) {
            orderMissCount.inc();
        } else {
            productionOrderNo = order.productionOrderNo;
            orderQuantity = order.orderQuantity;
            orderCreatedDate = order.createdDate;
            orderDueDate = order.dueDate;
        }

        // 4) 口径过滤二：订单号 NOT LIKE 'FG%'（沿用原报表，文档 2.4）
        if (productionOrderNo != null && productionOrderNo.startsWith(MesConstants.EXCLUDED_ORDER_PREFIX)) {
            filteredCount.inc();
            return;
        }

        // 5) 维表关联（维度 miss 是静默失败，每个维表单独计数）
        String lpnBarcode = null;
        String userName = null;
        String partCode = null;
        String partName = null;
        String lineCode = null;
        String lineName = null;
        String workCenterType = null;
        Long worksiteId = order == null ? null : order.worksiteId;

        if (barcode) {
            // 扫码：物料/用户名直接用字段；产线无 workcenterid 可查，linecode 留空（文档 5.4）
            lpnBarcode = value.lpnBarcode;
            userName = value.users;
            partCode = value.modelEncode;
            partName = value.model;
        } else {
            // 手动：物料经 order.partId 查 part；产线经 lot.workcenterid 查 workcenter；用户经 createdbyid 查 user
            lpnBarcode = value.lotNo;

            DimTables.Part part = partLookup.lookup(value.databaseName, order == null ? null : order.partId);
            if (part == null) {
                partMissCount.inc();
            } else {
                partCode = part.partCode;
                partName = part.partName;
            }

            DimTables.WorkCenter wc = localCache.workCenter(value.databaseName, value.workCenterId);
            if (wc == null) {
                workCenterMissCount.inc();
            } else {
                lineCode = wc.workCenterName;
                lineName = wc.description;
                workCenterType = wc.workCenterType;
                if (worksiteId == null) {
                    worksiteId = wc.worksiteId;   // workcenter 自带 worksiteid，作 workshop 兜底
                }
            }

            DimTables.User u = localCache.user(value.databaseName, value.createdById);
            if (u == null) {
                userMissCount.inc();
            } else {
                userName = u.userName;
            }
        }

        // workshop：经 order.worksiteId（两通道都有）或 workcenter.worksiteId（手动兜底）查 worksite
        String workshop = null;
        DimTables.WorkSite ws = localCache.workSite(value.databaseName, worksiteId);
        if (ws == null) {
            workSiteMissCount.inc();
        } else {
            workshop = ws.description;
        }

        // 6) 组装宽表记录
        long eventTime = value.eventTime();
        DwdReportRecord r = new DwdReportRecord();
        r.recordId = value.recordId;
        r.version = value.tsMs;
        r.isDeleted = isDeleted;
        r.company = value.company;
        r.dataSource = value.dataSource;
        r.outputType = value.outputType;
        r.productionOrder = productionOrderNo;           // 订单 miss 时为 null，留空输出
        r.productionOrderId = value.productionOrderId == null ? 0L : value.productionOrderId;
        r.lpnBarcode = lpnBarcode;
        r.userName = userName;
        r.partCode = partCode;
        r.partName = partName;
        r.lineCode = lineCode;
        r.lineName = lineName;
        r.workCenterType = workCenterType;
        r.workshop = workshop;
        r.orderQuantity = orderQuantity;
        r.orderCreatedDate = orderCreatedDate;
        r.orderDueDate = orderDueDate;
        r.lotQuantity = lotQuantity;
        r.outputDateTime = eventTime;
        r.shiftDate = ShiftUtil.shiftDateEpochDay(eventTime);
        out.collect(r);
    }

    @Override
    public void processBroadcastElement(DimUpdate value,
                                       Context ctx,
                                       Collector<DwdReportRecord> out) throws Exception {
        // 广播流承载两类更新（见 DimUpdate.kind）：静态维表定时全量快照 + 订单维 CDC 增量。
        // localCache 在 open() 已 bootstrap 兜底，这里就地更新后显式 put 回广播状态
        // （不依赖状态后端对"原地改对象"的检测，RocksDB 下也安全）。
        switch (value.kind) {
            case STATIC_SNAPSHOT:
                applyStaticSnapshot(value.cache);
                LOG.info("静态维表快照已刷新: {}", localCache.summary());
                break;
            case ORDER_UPSERT:
                localCache.ordersById
                        .computeIfAbsent(value.db, k -> new HashMap<>())
                        .put(value.orderId, value.order);
                break;
            case ORDER_DELETE:
                Map<Long, DimTables.ProductionOrder> orders = localCache.ordersById.get(value.db);
                if (orders != null) {
                    orders.remove(value.orderId);
                }
                break;
            default:
                throw new IllegalStateException("未知的维表广播类型: " + value.kind);
        }
        ctx.getBroadcastState(DIM_STATE_DESCRIPTOR).put(STATE_KEY, localCache);
    }

    @Override
    public void close() throws Exception {
        if (partLookup != null) {
            partLookup.close();
        }
        super.close();
    }

    /** 静态维表整份替换；订单维不动——订单由 CDC 增量维护，定时快照若覆盖订单会把最新变更冲回旧值 */
    private void applyStaticSnapshot(DimCache snap) {
        localCache.workCenters = snap.workCenters;
        localCache.workSites = snap.workSites;
        localCache.users = snap.users;
        localCache.loadedAt = snap.loadedAt;
    }
}
