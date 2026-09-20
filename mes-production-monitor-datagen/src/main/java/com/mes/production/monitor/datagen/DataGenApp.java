package com.mes.production.monitor.datagen;

import com.mes.production.monitor.common.util.JobConfig;
import com.mes.production.monitor.datagen.anomaly.AnomalyConfig;
import com.mes.production.monitor.datagen.anomaly.AnomalyInjector;
import com.mes.production.monitor.datagen.generator.DimGenerator;
import com.mes.production.monitor.datagen.generator.ReportGenerator;
import com.mes.production.monitor.datagen.writer.MysqlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        try (MysqlWriter writer = new MysqlWriter(
                conf.getString("mysql.jdbc.url.template"),
                conf.getString("mysql.username"),
                conf.getString("mysql.password"),
                conf.getInt("datagen.batch.size", 1000))) {

            DimGenerator dimGen = new DimGenerator(writer,
                    conf.getInt("datagen.workcenters.per.site", 8),
                    conf.getInt("datagen.part.count", 200),
                    conf.getInt("datagen.user.count", 50));
            ReportGenerator reportGen = new ReportGenerator(writer, anomalyConfig);
            AnomalyInjector injector = new AnomalyInjector(writer, anomalyConfig);

            LOG.info("造数模式: {}", mode);
            switch (mode) {
                case "dim":
                    forEachDatabase((db, company) -> dimGen.generate(db, company));
                    break;

                case "backfill":
                    // TODO(M1-5) 历史回填：
                    //   1) 先 dim（引用完整性顺序：维表 -> 订单 -> 报工）
                    //   2) reportGen.generateOrders(...)
                    //   3) reportGen.generateManualReports(...)
                    //   4) 旧库额外 reportGen.generateBarcodeReports(...)
                    //   规模参照文档 9.3：每基地 ~300 订单 / ~2000 手动报工 / ~3000 扫码，
                    //   跑通后把 datagen.scale 放大 10 倍造"规模感"。
                    throw new UnsupportedOperationException("TODO(M1-5): 见 switch 分支注释");

                case "streaming":
                    // TODO(M1-6) 持续实时模式：
                    //   为 10 个库各起一个线程跑 reportGen.runStreaming(...)，
                    //   主线程注册 shutdown hook 优雅退出。
                    throw new UnsupportedOperationException("TODO(M1-6): 见 switch 分支注释");

                case "anomaly":
                    // TODO(M1.5-6) 按 --anomaly.type 分派到 injector 的各个方法。
                    //   建议一次只注入一种，确认该类告警能被准确捕获后再开下一种。
                    throw new UnsupportedOperationException("TODO(M1.5-6): 见 switch 分支注释");

                default:
                    throw new IllegalArgumentException(
                            "未知 mode: " + mode + "，可选 dim / backfill / streaming / anomaly");
            }
            LOG.info("造数完成");
        }
    }

    /** 遍历 10 个库（5 基地 × 新旧 MES） */
    private static void forEachDatabase(DatabaseAction action) {
        for (String company : COMPANIES) {
            for (String prefix : new String[]{"mes_old_", "mes_new_"}) {
                action.apply(prefix + company, company);
            }
        }
    }

    @FunctionalInterface
    private interface DatabaseAction {
        void apply(String db, String company);
    }
}
