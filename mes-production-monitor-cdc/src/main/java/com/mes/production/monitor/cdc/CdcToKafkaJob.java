package com.mes.production.monitor.cdc;

import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业 1：MySQL CDC 采集 -&gt; Kafka {@code ods-mes-report}。见文档 6.1。
 *
 * <h3>设计要点</h3>
 * <ul>
 *   <li><b>两个 source 而不是十个</b>：10 个库在同一 MySQL 实例，用 {@code database-name} 正则
 *       一次匹配。source A 覆盖全部 10 库的 {@code lot}，source B 覆盖 5 个旧库的
 *       {@code it_barcodeautomatic}。见文档 2.1。</li>
 *   <li><b>基地标识靠元数据列</b>：{@code database_name METADATA} 解析出 company / datasource，
 *       不用给每个库写一份 DDL。</li>
 *   <li><b>server-id 必须配互不重叠的区间</b>：每个 source 的每个并行子任务占一个 id。
 *       冲突时报错是 {@code A slave with the same server_uuid/server_id ... has connected}，
 *       信息很迷惑。</li>
 * </ul>
 *
 * <h3>TODO 由你实现</h3>
 * 见下面各处 TODO。核心决策是 sink 的 changelog 语义（文档 6.1.1），这决定了下游能不能算对。
 */
public class CdcToKafkaJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcToKafkaJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("cdc.properties", args);

        // CDC 作业是纯 SQL 作业，不需要 RocksDB，用最朴素的 env 即可。
        // 但 checkpoint 一定要开：binlog offset 是存在 checkpoint 里的，
        // 不开 checkpoint 作业重启就只能从头全量。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(conf.getInt("job.parallelism", 2));
        env.enableCheckpointing(conf.getLong("checkpoint.interval.ms", 60_000L));
        env.getCheckpointConfig().setCheckpointStorage(conf.getString("checkpoint.dir"));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(lotSourceDdl(conf));
        tEnv.executeSql(barcodeSourceDdl(conf));

        // TODO(M2-1) 建 Kafka sink 表。
        //   这是本作业最关键的一个决策，先读文档 6.1.1 再动手。
        //
        //   lot 有 UPDATE（报工修正），CDC 产出的是 changelog 流。直接
        //     INSERT INTO kafka_sink SELECT * FROM ods_lot;
        //   会失败：
        //     TableException: Table sink 'xxx' doesn't support consuming update changes
        //   因为普通 'kafka' connector 是 append-only sink。
        //
        //   三条路，本项目选 B：
        //     A. 'upsert-kafka' + PRIMARY KEY  -> log compaction，只留最新值，丢了修正过程
        //     B. 'kafka' + 'format' = 'debezium-json'  -> 保留完整 changelog，下游能拿到 -U 旧值 【选这个】
        //     C. 'kafka' + 'format' = 'json' 只取新值  -> 表面能跑，但报工修正会被重复累加 【陷阱】
        //
        //   路线 C 是最容易误入的：Kafka 里有数据、看起来一切正常，
        //   只有做对账（文档 10.2）才会发现产量偏大。
        //
        //   sink DDL 需要的字段：见文档 5.2 的 ODS 部分 + OdsReportRecord 的元信息字段。
        // tEnv.executeSql(odsKafkaSinkDdl(conf));

        // TODO(M2-2) 两路 INSERT。
        //   要点：
        //   1. 用 STATEMENT SET 把两个 INSERT 放进同一个作业，否则会变成两个独立作业，
        //      两份 checkpoint、两套 binlog client，本地跑不动。
        //        StatementSet ss = tEnv.createStatementSet();
        //        ss.addInsertSql("INSERT INTO ... SELECT ... FROM ods_lot");
        //        ss.addInsertSql("INSERT INTO ... SELECT ... FROM ods_barcode");
        //        ss.execute();
        //   2. company / datasource 从 db_name 解析：
        //        REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 1)  -> old/new
        //        REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 2)  -> 基地
        //   3. record_id 两路不同：lot 用 CAST(lotid AS STRING)，扫码用 barcode。
        //   4. 本层【不做业务过滤】。qty>0、订单号 NOT LIKE 'FG%' 这些口径放到 DWD 层，
        //      ODS 保持原始，这样口径变更不用重跑 CDC。

        LOG.warn("CDC 作业骨架尚未完成，请先实现 M2-1 / M2-2 的 TODO");
        // env.execute("mes-production-monitor-cdc-ods");
    }

    /**
     * 全部 10 库的 {@code lot} 表。
     *
     * <p>注意几个选项的含义：
     * <ul>
     *   <li>{@code scan.startup.mode = initial}：全量 snapshot + 增量 binlog。
     *       开发期务必用这个；用 {@code latest-offset} 时如果没有新数据写入，
     *       你会看到作业正常运行但一条数据都没有，误以为配置错了。</li>
     *   <li>{@code scan.incremental.snapshot.chunk.size}：全量阶段的分片大小。
     *       调太大会 OOM，调太小会产生过多 split。</li>
     *   <li>{@code server-time-zone}：必须与 MySQL 的 time_zone 一致，
     *       否则 DATETIME 读出来偏移 8 小时（文档 12 节坑 #5）。</li>
     * </ul>
     */
    private static String lotSourceDdl(JobConfig conf) {
        return "CREATE TABLE ods_lot (\n"
                + "  lotid BIGINT,\n"
                + "  lotno STRING,\n"
                + "  productionorderid BIGINT,\n"
                + "  workcenterid BIGINT,\n"
                + "  finishedquantity DECIMAL(38, 4),\n"
                + "  createddate TIMESTAMP(3),\n"
                + "  createdbyid BIGINT,\n"
                // 元数据列：company / datasource 由它解析，见文档 2.1
                + "  db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "  tbl_name STRING METADATA FROM 'table_name' VIRTUAL,\n"
                + "  op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "  PRIMARY KEY (lotid) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'mysql-cdc',\n"
                + "  'hostname' = '" + conf.getString("mysql.hostname") + "',\n"
                + "  'port' = '" + conf.getString("mysql.port") + "',\n"
                + "  'username' = '" + conf.getString("mysql.cdc.username") + "',\n"
                + "  'password' = '" + conf.getString("mysql.cdc.password") + "',\n"
                // 正则匹配 10 个库
                + "  'database-name' = '" + conf.getString("cdc.lot.database.pattern") + "',\n"
                + "  'table-name' = 'lot',\n"
                // server-id 区间：与 barcode source 不重叠，长度 >= 并行度
                + "  'server-id' = '" + conf.getString("cdc.lot.server.id") + "',\n"
                + "  'scan.startup.mode' = '" + conf.getString("cdc.startup.mode", "initial") + "',\n"
                + "  'scan.incremental.snapshot.chunk.size' = '"
                + conf.getInt("cdc.snapshot.chunk.size", 8096) + "',\n"
                + "  'server-time-zone' = '" + conf.getString("cdc.server.timezone", "Asia/Shanghai") + "'\n"
                + ")";
    }

    /** 仅 5 个旧库的 {@code it_barcodeautomatic}（新 MES 没有扫码通道，见文档 2.2） */
    private static String barcodeSourceDdl(JobConfig conf) {
        return "CREATE TABLE ods_barcode (\n"
                + "  barcode STRING,\n"
                + "  lpnbarcode STRING,\n"
                + "  productionorderid BIGINT,\n"
                + "  modelencod STRING,\n"
                + "  model STRING,\n"
                + "  users STRING,\n"
                + "  quantity INT,\n"
                + "  scantime TIMESTAMP(3),\n"
                + "  db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "  tbl_name STRING METADATA FROM 'table_name' VIRTUAL,\n"
                + "  op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "  PRIMARY KEY (barcode) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'mysql-cdc',\n"
                + "  'hostname' = '" + conf.getString("mysql.hostname") + "',\n"
                + "  'port' = '" + conf.getString("mysql.port") + "',\n"
                + "  'username' = '" + conf.getString("mysql.cdc.username") + "',\n"
                + "  'password' = '" + conf.getString("mysql.cdc.password") + "',\n"
                + "  'database-name' = '" + conf.getString("cdc.barcode.database.pattern") + "',\n"
                + "  'table-name' = 'it_barcodeautomatic',\n"
                + "  'server-id' = '" + conf.getString("cdc.barcode.server.id") + "',\n"
                + "  'scan.startup.mode' = '" + conf.getString("cdc.startup.mode", "initial") + "',\n"
                + "  'scan.incremental.snapshot.chunk.size' = '"
                + conf.getInt("cdc.snapshot.chunk.size", 8096) + "',\n"
                + "  'server-time-zone' = '" + conf.getString("cdc.server.timezone", "Asia/Shanghai") + "'\n"
                + ")";
    }
}
