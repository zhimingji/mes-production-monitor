package com.mes.production.monitor.cdc;

import com.mes.production.monitor.common.constant.MesConstants;
import com.mes.production.monitor.common.constant.Topics;
import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 作业 1：MySQL CDC 采集 -&gt; Kafka {@code ods-mes-report}。见文档 6.1。
 *
 * <h3>设计要点</h3>
 * <ul>
 *   <li><b>两个 source 而不是十个</b>：10 个库在同一 MySQL 实例，用 {@code database-name} 正则
 *       一次匹配。source A 覆盖全部 10 库的 {@code lot}，source B 覆盖 5 个旧库的
 *       {@code barcodeautomatic}。见文档 2.1。</li>
 *   <li><b>基地标识靠元数据列</b>：{@code database_name METADATA} 解析出 company / datasource，
 *       不用给每个库写一份 DDL。</li>
 *   <li><b>server-id 必须配互不重叠的区间</b>：每个 source 的每个并行子任务占一个 id。
 *       冲突时报错是 {@code A slave with the same server_uuid/server_id ... has connected}，
 *       信息很迷惑。</li>
 * </ul>
 * 核心决策是 sink 的 changelog 语义（文档 6.1.1），这决定了下游能不能算对。
 */
public class CdcToKafkaJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcToKafkaJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig conf = JobConfig.load("cdc.properties", args);

        LOG.info("========== CDC 采集作业启动 ==========");
        LOG.info("并行度={}, checkpoint 周期={}ms, CDC 启动模式={}",
                conf.getInt("job.parallelism", 1),
                conf.getLong("checkpoint.interval.ms", 60_000L),
                conf.getString("cdc.startup.mode", "initial"));
        LOG.info("源 MySQL {}:{}, Kafka {}, 输出 topic: {} / {}",
                conf.getString("mysql.hostname"),
                conf.getString("mysql.port"),
                conf.getString("kafka.bootstrap.servers"),
                conf.getString("kafka.ods.report.topic", Topics.ODS_MES_REPORT),
                conf.getString("kafka.ods.order.topic", Topics.ODS_MES_ORDER));

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(conf.getInt("job.parallelism", 1));

        // 开启检查点, 默认周期为 1min
        env.enableCheckpointing(conf.getLong("checkpoint.interval.ms", 60_000L));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置检查点存储路径
        checkpointConfig.setCheckpointStorage(conf.getString("checkpoint.dir"));
        // 设置检查点模式为精确一次
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间,默认一分钟
        checkpointConfig.setCheckpointTimeout(conf.getLong("checkpoint.timeout.ms", 60_000L));
        // 取消作业时，checkpoint的数据保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 允许checkpoint连接失败的次数: 10次
        checkpointConfig.setTolerableCheckpointFailureNumber(conf.getInt("checkpoint.failure.number.tolerable", 10));
        // 开启非对齐检查点
        checkpointConfig.enableUnalignedCheckpoints();
        //如果大于0，一开始用 对齐检查点（Barrier对齐），对齐的时间超过这个参数，自动切换成 非对齐检查点（Barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        // 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(20, Time.days(1L), Time.minutes(1L)));

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建 lot cdc 表
        tableEnv.executeSql(lotSourceDdl(conf));
        LOG.info("已注册 CDC 源表 ods_lot");
        //创建 barcode cdc 源表
        tableEnv.executeSql(barcodeSourceDdl(conf));
        LOG.info("已注册 CDC 源表 ods_barcode");
        // 创建 order cdc 源表
        tableEnv.executeSql(orderSourceDdl(conf));
        LOG.info("已注册 CDC 源表 ods_order");

        // 创建 ods_mes_report kafka sink 表
        tableEnv.executeSql(reportKafkaSinkDdl(conf));
        LOG.info("已注册 Kafka sink 表 ods_mes_report (topic={})", conf.getString("kafka.ods.report.topic", Topics.ODS_MES_REPORT));
        // 创建 ods_mes_order kafka sink 表
        tableEnv.executeSql(orderKafkaSinkDdl(conf));
        LOG.info("已注册 Kafka sink 表 ods_mes_order (topic={})", conf.getString("kafka.ods.order.topic", Topics.ODS_MES_ORDER));

        // 创建语句集合
        StreamStatementSet statementSet = tableEnv.createStatementSet();

        statementSet.addInsertSql("INSERT INTO ods_mes_report\n" +
                "  SELECT\n" +
                "    CAST(lotid AS STRING) AS record_id,\n" +
                "    REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 2) AS company,\n" +
                "    CASE WHEN REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 1) = 'old' THEN 'OLD_MES' ELSE 'NEW_MES' END AS\n" +
                "  datasource,\n" +
                "    '手动报工' AS outputtype,\n" +
                "    productionorderid, lotid, lotno, workcenterid, finishedquantity, createddate, createdbyid,\n" +
                "    CAST(NULL AS STRING)  AS barcode,   -- lot 通道没有这些字段，补 NULL\n" +
                "    CAST(NULL AS STRING)  AS lpnbarcode,\n" +
                "    CAST(NULL AS STRING)  AS modelencode,\n" +
                "    CAST(NULL AS STRING)  AS model,\n" +
                "    CAST(NULL AS STRING)  AS users,\n" +
                "    CAST(NULL AS INT)     AS quantity,\n" +
                "    CAST(NULL AS TIMESTAMP(3)) AS scantime,\n" +
                "    db_name, tbl_name, opl_ts\n" +
                "  FROM ods_lot" +
                "  UNION ALL" +
                "  SELECT\n" +
                "    barcode                                             AS record_id,\n" +
                "    REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 2)  AS company,\n" +
                "    'OLD_MES'                                           AS datasource,\n" +
                "    '过线扫码'                                           AS outputtype,\n" +
                "    productionorderid,\n" +
                "    CAST(NULL AS BIGINT)        AS lotid,\n" +
                "    CAST(NULL AS STRING)        AS lotno,\n" +
                "    CAST(NULL AS BIGINT)        AS workcenterid,\n" +
                "    CAST(NULL AS DECIMAL(38,4)) AS finishedquantity,\n" +
                "    CAST(NULL AS TIMESTAMP(3))  AS createddate,\n" +
                "    CAST(NULL AS BIGINT)        AS createdbyid,\n" +
                "    barcode,                   \n" +
                "    lpnbarcode,\n" +
                "    modelencode,\n" +
                "    model,\n" +
                "    users,\n" +
                "    quantity,\n" +
                "    scantime,                   \n" +
                "    db_name,\n" +
                "    tbl_name,\n" +
                "    opl_ts\n" +
                "  FROM ods_barcode");


        statementSet.addInsertSql("INSERT INTO ods_mes_order\n" +
                "  SELECT\n" +
                "    productionorderid,\n" +
                "    REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 2) AS company,\n" +
                "    CASE WHEN REGEXP_EXTRACT(db_name, '^mes_(old|new)_(.*)$', 1) = 'old' THEN 'OLD_MES' ELSE 'NEW_MES' END AS datasource,\n" +
                "    productionorderno,\n" +
                "    orderquantity,\n" +
                "    worksiteid,\n" +
                "    partid,\n" +
                "    createddate,\n" +
                "    duedate,\n" +
                "    db_name,\n" +
                "    tbl_name,\n" +
                "    opl_ts\n" +
                "  FROM ods_order");

        // 执行语句集合
        LOG.info("提交 3 条 INSERT：lot / barcodeautomatic -> ods_mes_report，productionorder -> ods_mes_order");
        statementSet.execute();
        LOG.info("CDC 作业已提交，进入全量 snapshot + 增量 binlog 采集");
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
                + "  opl_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "  PRIMARY KEY (lotid) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'mysql-cdc',\n"
                + "  'hostname' = '" + conf.getString("mysql.hostname") + "',\n"
                + "  'port' = '" + conf.getString("mysql.port") + "',\n"
                + "  'username' = '" + conf.getString("mysql.cdc.username") + "',\n"
                + "  'password' = '" + conf.getString("mysql.cdc.password") + "',\n"
                // 正则匹配 10 个库
                + "  'database-name' = '" + conf.getString("cdc.lot.database.pattern") + "',\n"
                + "  'table-name' = '" + MesConstants.TABLE_LOT + "',\n"
                // server-id 区间：与 barcode source 不重叠，长度 >= 并行度
                + "  'server-id' = '" + conf.getString("cdc.lot.server.id") + "',\n"
                + "  'scan.startup.mode' = '" + conf.getString("cdc.startup.mode", "initial") + "',\n"
                + "  'scan.incremental.snapshot.chunk.size' = '"
                + conf.getInt("cdc.snapshot.chunk.size", 8096) + "',\n"
                + "  'server-time-zone' = '" + conf.getString("cdc.server.timezone", "Asia/Shanghai") + "',\n"
                + "  'jdbc.properties.allowPublicKeyRetrieval' = 'true',\n"
                + "  'jdbc.properties.useSSL' = 'false'\n"
                + ")";
    }

    /** 仅 5 个旧库的 {@code barcodeautomatic}（新 MES 没有扫码通道，见文档 2.2） */
    private static String barcodeSourceDdl(JobConfig conf) {
        return "CREATE TABLE ods_barcode (\n"
                + "  barcode STRING,\n"
                + "  lpnbarcode STRING,\n"
                + "  productionorderid BIGINT,\n"
                + "  modelencode STRING,\n"
                + "  model STRING,\n"
                + "  users STRING,\n"
                + "  quantity INT,\n"
                + "  scantime TIMESTAMP(3),\n"
                + "  db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "  tbl_name STRING METADATA FROM 'table_name' VIRTUAL,\n"
                + "  opl_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "  PRIMARY KEY (barcode) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'mysql-cdc',\n"
                + "  'hostname' = '" + conf.getString("mysql.hostname") + "',\n"
                + "  'port' = '" + conf.getString("mysql.port") + "',\n"
                + "  'username' = '" + conf.getString("mysql.cdc.username") + "',\n"
                + "  'password' = '" + conf.getString("mysql.cdc.password") + "',\n"
                + "  'database-name' = '" + conf.getString("cdc.barcode.database.pattern") + "',\n"
                + "  'table-name' = '" + MesConstants.TABLE_BARCODE + "',\n"
                + "  'server-id' = '" + conf.getString("cdc.barcode.server.id") + "',\n"
                + "  'scan.startup.mode' = '" + conf.getString("cdc.startup.mode", "initial") + "',\n"
                + "  'scan.incremental.snapshot.chunk.size' = '"
                + conf.getInt("cdc.snapshot.chunk.size", 8096) + "',\n"
                + "  'server-time-zone' = '" + conf.getString("cdc.server.timezone", "Asia/Shanghai") + "',\n"
                + "  'jdbc.properties.allowPublicKeyRetrieval' = 'true',\n"
                + "  'jdbc.properties.useSSL' = 'false'\n"
                + ")";
    }

    /** 创建 productionorder cdc 源表 */
    private static String orderSourceDdl(JobConfig conf) {
        return "CREATE TABLE ods_order(\n"
                + "  productionorderid BIGINT,\n"
                + "  productionorderno STRING,\n"
                + "  orderquantity BIGINT,\n"
                + "  worksiteid BIGINT,\n"
                + "  partid BIGINT,\n"
                + "  createddate TIMESTAMP(3),\n"
                + "  duedate TIMESTAMP(3),\n"
                + "  db_name STRING METADATA FROM 'database_name' VIRTUAL,\n"
                + "  tbl_name STRING METADATA FROM 'table_name' VIRTUAL,\n"
                + "  opl_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,\n"
                + "  PRIMARY KEY (productionorderid) NOT ENFORCED\n"
                + ")WITH(\n"
                + "  'connector' = 'mysql-cdc',\n"
                + "  'hostname' = '" + conf.getString("mysql.hostname") + "',\n"
                + "  'port' = '" + conf.getString("mysql.port") + "',\n"
                + "  'username' = '" + conf.getString("mysql.cdc.username") + "',\n"
                + "  'password' = '" + conf.getString("mysql.cdc.password") + "',\n"
                + "  'database-name' = '" + conf.getString("cdc.order.database.pattern") + "',\n"
                + "  'table-name' = '" + MesConstants.TABLE_PRODUCTION_ORDER + "',\n"
                + "  'server-id' = '" + conf.getString("cdc.order.server.id") + "',\n"
                + "  'scan.startup.mode' = '" + conf.getString("cdc.startup.mode", "initial") + "',\n"
                + "  'scan.incremental.snapshot.chunk.size' = '"
                + conf.getInt("cdc.snapshot.chunk.size", 8096) + "',\n"
                + "  'server-time-zone' = '" + conf.getString("cdc.server.timezone", "Asia/Shanghai") + "',\n"
                + "  'jdbc.properties.allowPublicKeyRetrieval' = 'true',\n"
                + "  'jdbc.properties.useSSL' = 'false'\n"
                + ")";
    }

    /** 创建 report kafka sink 表
     * 包含 lot 报工通道 和 barcodeautomatic 报工通道两种方式
     * */
    private static String reportKafkaSinkDdl(JobConfig conf) {
    return "CREATE TABLE ods_mes_report (\n"
            // 统一标识字段（两通道共有）
            + "  record_id STRING,\n"
            + "  company STRING,\n"
            + "  datasource STRING,\n"
            + "  outputtype STRING,\n"

            // 订单关联键（join 订单维表用，非唯一性）
            + "  productionorderid BIGINT,\n"

            // lot 通道字段
            + "  lotid BIGINT,\n"
            + "  lotno STRING,\n"
            + "  workcenterid BIGINT,\n"
            + "  finishedquantity DECIMAL(38, 4),\n"
            + "  createddate TIMESTAMP(3),\n"
            + "  createdbyid BIGINT,\n"

            // barcode 通道字段
            + "  barcode STRING,\n"
            + "  lpnbarcode STRING,\n"
            + "  modelencode STRING,\n"
            + "  model STRING,\n"
            + "  users STRING,\n"
            + "  quantity INT,\n"
            + "  scantime TIMESTAMP(3),\n"

            // CDC 来源信息
            + "  db_name STRING,\n"
            + "  tbl_name STRING,\n"
            + "  op_ts TIMESTAMP_LTZ(3)\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = '" + conf.getString("kafka.ods.report.topic", Topics.ODS_MES_REPORT) + "',\n"
            + "  'properties.bootstrap.servers' = '"
            + conf.getString("kafka.bootstrap.servers") + "',\n"
            + "  'key.format' = 'json',\n"
            + "  'key.fields' = 'company,datasource,record_id,outputtype',\n"
            + "  'value.format' = 'debezium-json',\n"
            + "  'sink.delivery-guarantee' = 'exactly-once',\n"
            // transaction 超时须 <= broker 的 transaction.max.timeout.ms（默认 15min），且 > checkpoint 周期（60s）
            + "  'properties.transaction.timeout.ms' = '300000',\n"
            + "  'sink.transactional-id-prefix' = 'mes-cdc-ods-report-'\n"
            + ")";
    }

    /**
     *  创建 order kafka sink 表
     *  包含 productionorder 生产订单
     */
    private static String orderKafkaSinkDdl(JobConfig conf) {
        return "CREATE TABLE ods_mes_order (\n"
                + "  productionorderid BIGINT,\n"
                + "  company STRING,\n"
                + "  datasource STRING,\n"
                + "  productionorderno STRING,\n"
                + "  orderquantity BIGINT,\n"
                + "  worksiteid BIGINT,\n"
                + "  partid BIGINT,\n"
                + "  createddate TIMESTAMP(3),\n"
                + "  duedate TIMESTAMP(3),\n"
                + "  db_name STRING,\n"
                + "  tbl_name STRING,\n"
                + "  op_ts TIMESTAMP_LTZ(3)\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = '" + conf.getString("kafka.ods.order.topic", Topics.ODS_MES_ORDER) + "',\n"
                + "  'properties.bootstrap.servers' = '" + conf.getString("kafka.bootstrap.servers") + "',\n"
                + "  'key.format' = 'json',\n"
                + "  'key.fields' = 'company,datasource,productionorderid',\n"
                + "  'value.format' = 'debezium-json',\n"
                + "  'sink.delivery-guarantee' = 'exactly-once',\n"
                // transaction 超时须 <= broker 的 transaction.max.timeout.ms（默认 15min），且 > checkpoint 周期（60s）
                + "  'properties.transaction.timeout.ms' = '300000',\n"
                + "  'sink.transactional-id-prefix' = 'mes-cdc-ods-order-'\n"
                + ")";
    }
}
