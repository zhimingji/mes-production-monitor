package com.mes.production.monitor.datagen;

import com.mes.production.monitor.common.constant.MesConstants;
import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.datagen.anomaly.AnomalyConfig;
import com.mes.production.monitor.datagen.anomaly.AnomalyInjector;
import com.mes.production.monitor.datagen.generator.DimGenerator;
import com.mes.production.monitor.datagen.generator.ReportGenerator;
import com.mes.production.monitor.datagen.writer.MysqlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 造数入口。见文档 9.3 / 9.3.1。
 *
 * <p>用法：
 * <pre>
 * # 1) 只造维表（10 个库）
 * java -jar mes-production-monitor-datagen.jar --mode dim
 *
 * # 2) 历史回填：造过去 7 天的订单与报工
 * java -jar mes-production-monitor-datagen.jar --mode backfill --backfill.days 7
 *
 * # 3) 持续实时（验证实时链路的唯一手段，跑起来别关）
 * java -jar mes-production-monitor-datagen.jar --mode streaming
 *
 * # 4) 单独注入某类异常
 * java -jar mes-production-monitor-datagen.jar --mode anomaly --anomaly.type line-idle
 * </pre>
 *
 * <p><b>注意</b>：{@code backfill} 造出来的是历史时间戳，实时链路会判定为迟到数据。
 * 先跑 backfill 建立对账基线，再跑 streaming 验证实时语义，两者用途不同。
 */
public class DataGenApp {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenApp.class);

    private static final String[] COMPANIES = {"zh", "zz", "hf", "wh", "cq"};

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("datagen.properties", args);
        String mode = conf.getString("mode", "dim");

        AnomalyConfig anomalyConfig = AnomalyConfig.from(conf);

        int scale = conf.getInt("datagen.scale", 1);
        int workCentersPerSite = conf.getInt("datagen.workcenters.per.site", 8);
        int partCount = conf.getInt("datagen.part.count", 200);
        int userCount = conf.getInt("datagen.user.count", 50);
        int orderCount = scale * conf.getInt("datagen.order.count", 300);
        int manualCount = scale * conf.getInt("datagen.manual.report.count", 2000);
        int barcodeCount = scale * conf.getInt("datagen.barcode.report.count", 3000);
        int recordsPerLpn = conf.getInt("datagen.barcode.records.per.lpn", 5);
        int dayBatch = conf.getInt("streaming.day.shift.batch", 20);
        int nightBatch = conf.getInt("streaming.night.shift.batch", 4);

        try (MysqlWriter writer = new MysqlWriter(
                conf.getString("mysql.jdbc.url.template"),
                conf.getString("mysql.username"),
                conf.getString("mysql.password"),
                conf.getInt("datagen.batch.size", 1000))) {

            DimGenerator dimGen = new DimGenerator(writer,
                    workCentersPerSite, partCount, userCount);
            ReportGenerator reportGen = new ReportGenerator(writer, anomalyConfig,
                    workCentersPerSite, partCount, userCount, orderCount, recordsPerLpn,
                    dayBatch, nightBatch);
            AnomalyInjector injector = new AnomalyInjector(writer, anomalyConfig);

            LOG.info("造数模式: {}", mode);
            switch (mode) {
                case "dim":
                    forEachDatabase((db, company) -> dimGen.generate(db, company));
                    break;

                case "backfill": {
                    long to = System.currentTimeMillis();
                    long from = to - conf.getLong("backfill.days", 7) * 24L * 3600_000L;
                    // 引用完整性顺序：维表 -> 订单 -> 报工；旧库额外扫码（文档 9.3.1）
                    forEachDatabase((db, company) -> {
                        dimGen.generate(db, company);
                        reportGen.generateOrders(db, company, orderCount, from, to);
                        reportGen.generateManualReports(db, company, manualCount, from, to);
                        if (db.startsWith(MesConstants.DB_PREFIX_OLD)) {
                            reportGen.generateBarcodeReports(db, company, barcodeCount, from, to);
                        }
                    });
                    break;
                }

                case "streaming": {
                    long intervalMs = conf.getLong("streaming.interval.ms", 1000);
                    List<Thread> threads = new ArrayList<>();
                    for (String company : COMPANIES) {
                        for (String prefix : new String[]{MesConstants.DB_PREFIX_OLD, MesConstants.DB_PREFIX_NEW}) {
                            String db = prefix + company;
                            Thread t = new Thread(() -> reportGen.runStreaming(db, company, intervalMs),
                                    "stream-" + db);
                            t.start();
                            threads.add(t);
                        }
                    }
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        LOG.info("收到退出信号，停止 streaming");
                        reportGen.stop();
                    }));
                    LOG.info("streaming 已启动 {} 个库的写入线程，Ctrl+C 退出", threads.size());
                    for (Thread t : threads) {
                        t.join();
                    }
                    break;
                }

                case "anomaly": {
                    String type = conf.getString("anomaly.type", "correction");
                    // 建议一次只注入一种，确认该类告警能被准确捕获后再开下一种（文档 9.3.1）
                    forEachDatabase((db, company) -> dispatchAnomaly(injector, anomalyConfig, type, db));
                    break;
                }

                default:
                    throw new IllegalArgumentException(
                            "未知 mode: " + mode + "，可选 dim / backfill / streaming / anomaly");
            }
            LOG.info("造数完成");
        }
    }

    private static void dispatchAnomaly(AnomalyInjector injector, AnomalyConfig cfg, String type, String db) {
        switch (type) {
            case "correction":
                injector.injectCorrection(db);
                break;
            case "line-idle":
                injector.injectLineIdle(db, cfg.idleLineCount, cfg.idleDurationMinutes);
                break;
            case "over-report":
                injector.injectOverReport(db);
                break;
            case "lag":
                injector.injectLag(db);
                break;
            case "out-of-order":
                injector.injectOutOfOrder(db);
                break;
            default:
                throw new IllegalArgumentException("未知 anomaly.type: " + type
                        + "（可选 correction / line-idle / over-report / lag / out-of-order）");
        }
    }

    /** 遍历 10 个库（5 基地 × 新旧 MES） */
    private static void forEachDatabase(DatabaseAction action) {
        for (String company : COMPANIES) {
            for (String prefix : new String[]{MesConstants.DB_PREFIX_OLD, MesConstants.DB_PREFIX_NEW}) {
                action.apply(prefix + company, company);
            }
        }
    }

    @FunctionalInterface
    private interface DatabaseAction {
        void apply(String db, String company);
    }
}
