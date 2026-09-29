package com.mes.production.monitor.dwd.function;

import com.mes.production.monitor.common.metric.MetricNames;
import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.OdsReportRecord;
import com.mes.production.monitor.dwd.dim.DimCache;
import com.mes.production.monitor.dwd.dim.DimLoader;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DWD 核心算子：维表广播 join + 订单关联 + 口径过滤 + 双通道统一。见文档 6.2。
 *
 * <h3>已经替你处理好的：广播初始化竞态</h3>
 * {@code open()} 里先用 {@link DimLoader} 做一次 JDBC 全量 bootstrap，
 * 因此第一条数据到来时本地缓存一定是就绪的，不存在"广播还没到导致维度全空"的窗口。
 * 广播流只负责后续增量刷新。见文档 6.2.1。
 *
 * <h3>已经替你埋好的：维度 miss 计数</h3>
 * 维度 join 不上是<b>静默失败</b>的典型——数据条数一条不少，只是维度字段全空。
 * 各维表分别有 Counter，Flink Web UI 里能直接看到。
 *
 * <h3>TODO 由你实现：{@link #processElement}</h3>
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
        extends BroadcastProcessFunction<OdsReportRecord, DimCache, DwdReportRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DimBroadcastJoinFunction.class);

    /** 广播状态描述符。作业侧 {@code stream.broadcast(DESCRIPTOR)} 要用同一个实例 */
    public static final MapStateDescriptor<String, DimCache> DIM_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("dim-cache", String.class, DimCache.class);

    private static final String STATE_KEY = "ALL";

    private final String jdbcUrlTemplate;
    private final String username;
    private final String password;

    /** 本地缓存：open() bootstrap 得到，广播到达后被替换 */
    private transient DimCache localCache;

    private transient Counter dirtyCount;
    private transient Counter filteredCount;
    private transient Counter orderMissCount;
    private transient Counter partMissCount;
    private transient Counter workCenterMissCount;
    private transient Counter workSiteMissCount;
    private transient Counter userMissCount;

    public DimBroadcastJoinFunction(String jdbcUrlTemplate, String username, String password) {
        this.jdbcUrlTemplate = jdbcUrlTemplate;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 关键：bootstrap 兜底，消除广播竞态窗口（文档 6.2.1）
        this.localCache = DimLoader.loadAll(jdbcUrlTemplate, username, password);
        LOG.info("subtask {} 维表 bootstrap 完成: {}",
                getRuntimeContext().getIndexOfThisSubtask(), localCache.summary());

        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup(MetricNames.GROUP);
        this.dirtyCount = g.counter(MetricNames.DIRTY_RECORD_COUNT);
        this.filteredCount = g.counter(MetricNames.FILTERED_RECORD_COUNT);
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
        // TODO(M3-1) 实现两通道解析 + 订单关联 + 维表关联 + 口径过滤。
        //
        // 建议的实现顺序（先跑通手动报工通道，再加扫码通道）：
        //
        // 1) changelog：先看 value.op
        //      "c" / "r" -> 新增或全量快照，正常处理
        //      "u"       -> 报工修正。record_id 不变、version(tsMs) 变大，
        //                   下游 ClickHouse ReplacingMergeTree 会自动取最新，
        //                   所以这里【直接按新值输出即可】，不要自己做加减。
        //      "d"       -> 物理删除。生产环境报工很少物理删，但要想清楚怎么处理
        //                   （方案：ReplacingMergeTree 加 is_deleted 列 + 查询侧过滤）。
        //
        // 2) 订单关联（文档 2.4 第 4 条）：
        //      手动报工与过线扫码都使用：
        //      value.productionOrderId -> localCache.order(db, id).productionOrderNo
        //      两个源表的订单键类型与语义一致，不再需要按订单编号反查。
        //      两边任一侧查不到订单 -> orderMissCount.inc() 并决定是丢弃还是留空维度输出
        //      （建议：留空输出 + 计数，不要丢，否则对账时数量对不上还找不到原因）
        //
        // 3) 维表关联：
        //      partcode/partname：手动报工经 order.partId 查 part；扫码直接用 modelencode/model
        //      linecode/linename：手动报工用 lot.workcenterid 查 workcenter
        //      workshop：经 order.worksiteId 或 workcenter.worksiteId 查 worksite
        //      username：手动报工用 createdbyid 查 user；扫码 users 字段本身就是用户名
        //
        // 4) 口径过滤（沿用原报表，文档 2.4）：
        //      lotquantity > 0
        //      productionorder NOT LIKE 'FG%'   -> MesConstants.EXCLUDED_ORDER_PREFIX
        //      每次过滤都要 filteredCount.inc()
        //
        // 5) 填 record_id / version / shift_date：
        //      recordId = 手动报工 CAST(lotid) / 扫码 barcode
        //      version  = value.tsMs
        //      shiftDate = ShiftUtil.shiftDateEpochDay(value.eventTime())
        //
        // 6) 解析不出关键字段的 -> dirtyCount.inc() 后 return，不要抛异常让作业挂掉。
        throw new UnsupportedOperationException("TODO(M3-1): 见方法内注释");
    }

    @Override
    public void processBroadcastElement(DimCache value,
                                       Context ctx,
                                       Collector<DwdReportRecord> out) throws Exception {
        // 定期全量刷新：整份替换。维表小，全量替换比增量合并简单且不会有一致性问题。
        ctx.getBroadcastState(DIM_STATE_DESCRIPTOR).put(STATE_KEY, value);
        this.localCache = value;
        LOG.info("维表广播刷新: {}", value.summary());
    }
}
