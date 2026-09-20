package com.mes.production.monitor.datagen.generator;

import com.mes.production.monitor.datagen.anomaly.AnomalyConfig;
import com.mes.production.monitor.datagen.writer.MysqlWriter;

/**
 * 报工造数：productionorder + lot（手动报工）+ it_barcodeautomatic（过线扫码）。
 *
 * <h3>两种模式（文档 9.3.1）</h3>
 * <ul>
 *   <li>{@code backfill}：一次性造出过去 N 天数据后退出。验证 CDC 全量 snapshot、对账基线。</li>
 *   <li>{@code streaming}：常驻进程，按真实节奏持续写入。
 *       <b>这是验证实时链路的唯一手段</b>——没有持续流入，watermark 不推进、窗口不触发、
 *       告警 Timer 不响，你会以为代码写对了，其实一个实时语义都没验证过。</li>
 * </ul>
 *
 * <h3>真实节奏（参照压缩机厂）</h3>
 * <ul>
 *   <li>白班 08:30–17:30 密集报工，晚班稀疏（约白班的 1/5）</li>
 *   <li>过线扫码集中在总装线，且单个 LPN 会陆续产生多条扫码记录
 *       —— 这正是 DWD 层"聚合边界"问题的来源（文档 6.2.2），造数时要真的造出这个特征</li>
 *   <li>同一订单的报工跨多个批次，达成率逐步爬升</li>
 * </ul>
 */
public class ReportGenerator {

    private final MysqlWriter writer;
    private final AnomalyConfig anomaly;

    public ReportGenerator(MysqlWriter writer, AnomalyConfig anomaly) {
        this.writer = writer;
        this.anomaly = anomaly;
    }

    /**
     * TODO(M1-1) 生成生产订单。
     *
     * <p>要点：
     * <ul>
     *   <li>{@code productionorderno} 全局唯一且可读，建议 {@code PO + 基地缩写 + yyyyMMdd + 序号}。</li>
     *   <li><b>刻意造一批 {@code FG} 开头的订单号</b>：原报表口径要过滤 {@code NOT LIKE 'FG%'}，
     *       不造这类数据的话过滤逻辑等于没测（文档 2.4）。</li>
     *   <li>{@code createddate} 和 {@code duedate} 决定计划爬坡曲线，生产周期建议 3~15 天随机。</li>
     *   <li>刻意造少量 {@code duedate <= createddate} 的脏数据，验证计划进度计算的除零防护。</li>
     *   <li>{@code worksiteid} / {@code partid} 必须引用真实存在的维表记录。</li>
     * </ul>
     */
    public void generateOrders(String db, String company, int orderCount, long fromMillis, long toMillis) {
        throw new UnsupportedOperationException("TODO(M1-1): 见方法注释");
    }

    /**
     * TODO(M1-2) 生成手动报工（lot）。
     *
     * <p>要点：
     * <ul>
     *   <li>按班次节奏分布 {@code createddate}：白班密集、晚班稀疏，不要均匀随机
     *       —— 均匀分布测不出真实的窗口和倾斜行为。</li>
     *   <li>同一订单拆成多个批次报工，累计量逐步接近 {@code orderquantity}。</li>
     *   <li>刻意造少量 {@code finishedquantity <= 0} 的记录，验证 DWD 的过滤口径。</li>
     *   <li>{@code workcenterid} 引用真实产线；产线之间的量要<b>刻意不均匀</b>
     *       （总装线远高于其他线），这样才能在 Flink Web UI 里看到真实的数据倾斜（文档 11.1）。</li>
     * </ul>
     */
    public void generateManualReports(String db, String company, int reportCount,
                                     long fromMillis, long toMillis) {
        throw new UnsupportedOperationException("TODO(M1-2): 见方法注释");
    }

    /**
     * TODO(M1-3) 生成过线扫码（it_barcodeautomatic，仅旧 MES）。
     *
     * <p>要点：
     * <ul>
     *   <li><b>同一个 {@code lpnbarcode} 必须产生多条扫码记录</b>，且 {@code scantime} 陆续递增。
     *       这是原报表要按 LPN 分组 {@code SUM(quantity)}、{@code MAX(scantime)} 的原因，
     *       也是流上"聚合边界"问题的来源。造数时不造出这个特征，你就体会不到那个问题。</li>
     *   <li>{@code productionorderid} 存生产订单 ID，与 lot 通道语义和类型一致；
     *       两个通道都通过它关联 productionorder 表取得 productionorderno。</li>
     *   <li>{@code modelencod} / {@code model} 对应 partcode / partname，
     *       要与 part 维表里的值一致，否则对账时两个通道的物料口径对不上。</li>
     *   <li>{@code users} 存的是用户名字符串（不是 userid），与 lot 通道不同。</li>
     *   <li>扫码集中在总装线。</li>
     * </ul>
     */
    public void generateBarcodeReports(String db, String company, int reportCount,
                                      long fromMillis, long toMillis) {
        throw new UnsupportedOperationException("TODO(M1-3): 见方法注释");
    }

    /**
     * TODO(M1-4) streaming 模式：常驻循环，按真实节奏持续写入。
     *
     * <p>实现思路：
     * <pre>
     * while (running) {
     *     long now = System.currentTimeMillis();
     *     int batch = 当前时段的报工强度(now);   // 白班多、晚班少
     *     写入 batch 条 lot（+ 总装线的扫码）
     *     anomaly.maybeInject(now);            // 按配置注入异常
     *     Thread.sleep(间隔);
     * }
     * </pre>
     *
     * <p><b>报工时间用 {@code now} 而不是历史时间</b>：实时链路的 watermark 依赖事件时间接近当前时间，
     * 塞历史时间戳会让窗口立刻判定为迟到。
     */
    public void runStreaming(String db, String company, long intervalMs) {
        throw new UnsupportedOperationException("TODO(M1-4): 见方法注释");
    }
}
