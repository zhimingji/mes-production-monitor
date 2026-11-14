package com.mes.production.monitor.datagen.generator;

import com.mes.production.monitor.common.constant.MesConstants;
import com.mes.production.monitor.datagen.anomaly.AnomalyConfig;
import com.mes.production.monitor.datagen.writer.MysqlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 报工造数：productionorder + lot（手动报工）+ barcodeautomatic（过线扫码）。
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
 *
 * <h3>确定性（同基地新旧库内容相同）</h3>
 * 随机种子只由 {@code company} 决定、不掺 {@code db}，所以 {@code mes_old_zh} 与 {@code mes_new_zh}
 * 会造出<b>完全一致</b>的订单与报工——这正是文档 2.2「同基地新旧库内容相同」的语义来源，
 * 也是下游必须按 {@code dataSource} 隔离订单键的原因（见文档 7.2）。
 */
public class ReportGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ReportGenerator.class);

    /** 车间/分厂数量，与 {@link DimGenerator#WORKSHOPS} 一致 */
    private static final int WORKSHOP_COUNT = 7;
    private static final ZoneId BIZ_ZONE = MesConstants.BIZ_ZONE;

    /** 白班报工占比（晚班约 1/5，模拟真实节奏） */
    private static final double DAY_SHIFT_RATIO = 0.8;
    /** 总装线承接的报工占比——刻意不均匀，Flink Web UI 里能看到真实数据倾斜（文档 11.1） */
    private static final double ASSEMBLY_RATIO = 0.8;

    private static final String ORDER_INSERT_SQL = "INSERT IGNORE INTO productionorder "
            + "(productionorderid, productionorderno, orderquantity, worksiteid, partid, createddate, duedate) "
            + "VALUES (?,?,?,?,?,?,?)";
    private static final String LOT_INSERT_SQL = "INSERT IGNORE INTO lot "
            + "(lotid, lotno, productionorderid, workcenterid, finishedquantity, createddate, createdbyid) "
            + "VALUES (?,?,?,?,?,?,?)";
    private static final String BARCODE_INSERT_SQL = "INSERT IGNORE INTO barcodeautomatic "
            + "(barcode, lpnbarcode, productionorderid, modelencode, model, users, quantity, scantime) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final MysqlWriter writer;
    private final AnomalyConfig anomaly;
    private final int workCentersPerSite;
    private final int partCount;
    private final int userCount;
    private final int orderCount;
    private final int barcodeRecordsPerLpn;
    private final int dayShiftBatch;
    private final int nightShiftBatch;

    /** streaming 的开关：shutdown hook 置 false，各写入线程在下一轮循环退出 */
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ReportGenerator(MysqlWriter writer, AnomalyConfig anomaly,
                           int workCentersPerSite, int partCount, int userCount,
                           int orderCount, int barcodeRecordsPerLpn,
                           int dayShiftBatch, int nightShiftBatch) {
        this.writer = writer;
        this.anomaly = anomaly;
        this.workCentersPerSite = workCentersPerSite;
        this.partCount = partCount;
        this.userCount = userCount;
        this.orderCount = orderCount;
        this.barcodeRecordsPerLpn = barcodeRecordsPerLpn;
        this.dayShiftBatch = dayShiftBatch;
        this.nightShiftBatch = nightShiftBatch;
    }

    public void stop() {
        running.set(false);
    }

    public void generateOrders(String db, String company, int orderCount, long fromMillis, long toMillis) {
        Random rnd = random(company);
        List<Object[]> rows = new ArrayList<>(orderCount);
        for (int i = 1; i <= orderCount; i++) {
            long created = randomMillis(rnd, fromMillis, toMillis);
            // 生产周期 3~15 天：createddate/duedate 决定计划爬坡曲线的斜率
            long due = created + (3 + rnd.nextInt(13)) * 24L * 3600_000L;
            // 脏数据：少量 duedate <= createddate，验证计划进度计算的除零/倒挂防护
            if (rnd.nextDouble() < 0.01) {
                due = created - (1 + rnd.nextInt(3)) * 3600_000L;
            }
            long quantity = 100 + rnd.nextInt(901); // 100..1000
            boolean fg = rnd.nextDouble() < anomaly.fgOrderRatio;
            boolean dangling = rnd.nextDouble() < anomaly.danglingFkRatio;
            long worksiteId = dangling ? 9999L : 1 + rnd.nextInt(WORKSHOP_COUNT);
            long partId = dangling ? 999999L : 1 + rnd.nextInt(partCount);
            // 订单号可读：FG 前缀用于验证 DWD 的 NOT LIKE 'FG%' 过滤口径
            String date = YYYYMMDD.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(created), BIZ_ZONE));
            String orderNo = (fg ? "FG" : "PO") + "-" + company.toUpperCase() + "-" + date + "-"
                    + String.format("%04d", i);
            rows.add(new Object[]{(long) i, orderNo, quantity, worksiteId, partId, toLdt(created), toLdt(due)});
        }
        writer.batch(db, ORDER_INSERT_SQL, rows);
        LOG.info("[{}] 订单造数完成: {} 条", db, orderCount);
    }

    public void generateManualReports(String db, String company, int reportCount,
                                      long fromMillis, long toMillis) {
        Random rnd = random(company);
        List<Object[]> rows = new ArrayList<>(reportCount);
        for (int i = 1; i <= reportCount; i++) {
            long orderId = 1 + rnd.nextInt(orderCount);       // 同一订单被多次命中 => 跨批次报工
            long workcenterId = pickWorkCenter(rnd);          // 总装线占大头，制造数据倾斜
            BigDecimal qty = BigDecimal.valueOf(1 + rnd.nextInt(50));
            if (rnd.nextDouble() < anomaly.nonPositiveQtyRatio) {
                qty = BigDecimal.valueOf(-rnd.nextInt(5));    // finishedquantity <= 0，验证 DWD 过滤
            }
            long created = randomReportTime(rnd, fromMillis, toMillis); // 白班密、晚班稀
            long userId = 1 + rnd.nextInt(userCount);
            String lotNo = "LOT-" + company.toUpperCase() + "-" + String.format("%06d", i);
            rows.add(new Object[]{(long) i, lotNo, orderId, workcenterId, qty, toLdt(created), userId});
        }
        writer.batch(db, LOT_INSERT_SQL, rows);
        LOG.info("[{}] 手动报工造数完成: {} 条", db, reportCount);
    }

    public void generateBarcodeReports(String db, String company, int reportCount,
                                       long fromMillis, long toMillis) {
        Random rnd = random(company);
        List<Object[]> rows = new ArrayList<>(reportCount);
        long barcodeSeq = 0;
        int lpnIdx = 0;
        while (barcodeSeq < reportCount) {
            lpnIdx++;
            String lpn = "LPN-" + company.toUpperCase() + "-" + String.format("%06d", lpnIdx);
            // 同一个 LPN 内，productionorderid / 物料 / 用户恒定，只有 scantime 陆续递增——
            // 这正是原报表要按 LPN 分组 SUM(quantity)、MAX(scantime) 的根因。
            long orderId = 1 + rnd.nextInt(orderCount);
            int partIdx = 1 + rnd.nextInt(partCount);
            String modelEncode = String.format("PN%06d", partIdx);
            String model = "压缩机部件-" + partIdx;
            String user = company + "_op" + String.format("%03d", 1 + rnd.nextInt(userCount));
            long baseScan = randomReportTime(rnd, fromMillis, toMillis);
            int recordsThisLpn = (int) Math.min(barcodeRecordsPerLpn, reportCount - barcodeSeq);
            for (int k = 0; k < recordsThisLpn; k++) {
                long scanTime = baseScan + k * (30_000L + rnd.nextInt(120_000)); // 陆续递增
                String barcode = "BC-" + company.toUpperCase() + "-" + String.format("%08d", ++barcodeSeq);
                int quantity = 1 + rnd.nextInt(10);
                rows.add(new Object[]{barcode, lpn, orderId, modelEncode, model, user, quantity, toLdt(scanTime)});
            }
        }
        writer.batch(db, BARCODE_INSERT_SQL, rows);
        LOG.info("[{}] 扫码报工造数完成: {} 条", db, reportCount);
    }

    public void runStreaming(String db, String company, long intervalMs) {
        Random rnd = random(company);
        boolean isOld = db.startsWith(MesConstants.DB_PREFIX_OLD);
        // 大基数 + 时间前缀：lotid/barcode 与 backfill 的 1..N 不撞车，也无需查询 MAX(lotid)
        long idBase = System.currentTimeMillis() * 1000L;
        long lotSeq = 0;
        long barcodeSeq = 0;

        LOG.info("[{}] streaming 开始: interval={}ms, 白班={}/轮, 晚班={}/轮",
                db, intervalMs, dayShiftBatch, nightShiftBatch);
        while (running.get()) {
            long now = System.currentTimeMillis();
            int batch = isDayShift(now) ? dayShiftBatch : nightShiftBatch;

            List<Object[]> lots = new ArrayList<>(batch);
            for (int b = 0; b < batch; b++) {
                long lotId = idBase + (++lotSeq);
                BigDecimal qty = BigDecimal.valueOf(1 + rnd.nextInt(50));
                if (rnd.nextDouble() < anomaly.nonPositiveQtyRatio) {
                    qty = BigDecimal.valueOf(-rnd.nextInt(5));
                }
                lots.add(new Object[]{lotId, "LOT-" + company.toUpperCase() + "-S" + lotId,
                        1 + rnd.nextInt(orderCount), pickWorkCenter(rnd), qty,
                        toLdt(now), 1 + rnd.nextInt(userCount)});
            }
            writer.batch(db, LOT_INSERT_SQL, lots);

            // 旧库额外过线扫码：同一 LPN 一批多次，scantime 陆续递增
            if (isOld) {
                int scanBatch = Math.max(1, batch / 2);
                List<Object[]> scans = new ArrayList<>(scanBatch);
                String lpn = "LPN-" + company.toUpperCase() + "-S" + (idBase + (++barcodeSeq));
                for (int b = 0; b < scanBatch; b++) {
                    int partIdx = 1 + rnd.nextInt(partCount);
                    scans.add(new Object[]{
                            "BC-" + company.toUpperCase() + "-S" + (idBase + (++barcodeSeq)),
                            lpn, 1 + rnd.nextInt(orderCount),
                            String.format("PN%06d", partIdx), "压缩机部件-" + partIdx,
                            company + "_op" + String.format("%03d", 1 + rnd.nextInt(userCount)),
                            1 + rnd.nextInt(10), toLdt(now + b * 30_000L)});
                }
                writer.batch(db, BARCODE_INSERT_SQL, scans);
            }

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.info("[{}] streaming 结束", db);
    }

    // ==================== helpers ====================

    /** 只按 company 取种子：同基地新旧库造出完全一致的数据（文档 2.2） */
    private static Random random(String company) {
        return new Random(company.hashCode() * 31L);
    }

    private static long randomMillis(Random rnd, long from, long to) {
        return from + (long) (rnd.nextDouble() * (to - from));
    }

    /** 产线刻意不均匀：总装线（siteIdx 0 和 6，见 DimGenerator 的类型映射）占 ASSEMBLY_RATIO */
    private long pickWorkCenter(Random rnd) {
        int siteIdx;
        if (rnd.nextDouble() < ASSEMBLY_RATIO) {
            siteIdx = rnd.nextBoolean() ? 0 : (WORKSHOP_COUNT - 1);
        } else {
            siteIdx = 1 + rnd.nextInt(WORKSHOP_COUNT - 2); // 1..5
        }
        int k = 1 + rnd.nextInt(workCentersPerSite);
        return (long) siteIdx * workCentersPerSite + k;
    }

    /** 报工时间按班次分布：80% 白班（08:30~17:30），20% 晚班（17:30~次日 08:30） */
    private static long randomReportTime(Random rnd, long fromMillis, long toMillis) {
        long fromDay = Instant.ofEpochMilli(fromMillis).atZone(BIZ_ZONE).toLocalDate().toEpochDay();
        long toDay = Instant.ofEpochMilli(toMillis).atZone(BIZ_ZONE).toLocalDate().toEpochDay();
        LocalDate date = LocalDate.ofEpochDay(fromDay + (long) (rnd.nextDouble() * (toDay - fromDay + 1)));
        if (rnd.nextDouble() < DAY_SHIFT_RATIO) {
            LocalTime time = LocalTime.of(8, 30).plusMinutes(rnd.nextInt(9 * 60));
            return LocalDateTime.of(date, time).atZone(BIZ_ZONE).toInstant().toEpochMilli();
        }
        long start = LocalDateTime.of(date, LocalTime.of(17, 30)).atZone(BIZ_ZONE).toInstant().toEpochMilli();
        return start + (long) (rnd.nextDouble() * (15 * 60 * 60_000L));
    }

    /** 白班 = 08:30~17:30（与班次日界 08:30 对齐），其余为晚班 */
    private static boolean isDayShift(long epochMillis) {
        LocalTime t = LocalTime.from(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), BIZ_ZONE));
        int m = t.getHour() * 60 + t.getMinute();
        return m >= 8 * 60 + 30 && m < 17 * 60 + 30;
    }

    /** epoch millis -> 北京时间 LocalDateTime（DATETIME 列无时区，直接存墙钟时间，见文档 12 坑 #5） */
    private static LocalDateTime toLdt(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), BIZ_ZONE);
    }
}
