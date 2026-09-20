# mes-production-monitor · MES五地生产报工实时监控预警平台

项目背景：把公司原有的「30 分钟批MES五地生产报表」改造成秒级实时链路：
**Flink CDC → Kafka → Flink → ClickHouse**。

- 详细设计：[技术设计文档.md](./技术设计文档.md)
- 踩坑记录：[TROUBLESHOOTING.md](./TROUBLESHOOTING.md) ← 每踩一个坑就记一条

---

## 技术栈

| 组件 | 版本         | 备注              |
|---|------------|-----------------|
| Flink | 1.18.1     |                 |
| Flink CDC | 3.2.1      |                 |
| flink-connector-kafka | 3.0.2-1.18 |                 |
| Kafka | 3.6.1      | kafka集群 3节点     |
| ClickHouse | 0.6.0      |                 |
| MySQL | 8.0        | 10 个 MES 库放在同一实例 |
| Java | 8          | 与集群一致  |

---

## 工程结构

```
mes-production-monitor/
├── pom.xml                 父 POM：版本矩阵集中管理
├── mes-production-monitor-common/          POJO、枚举、配置加载、班次工具、ClickHouse 攒批 Sink
├── mes-production-monitor-datagen/         造数：backfill / streaming / 异常注入
├── mes-production-monitor-cdc/             作业1  10 库 CDC        → Kafka ods-mes-report
├── mes-production-monitor-dwd/             作业2  维表广播 join    → Kafka dwd-mes-report + CK
├── mes-production-monitor-dws/             作业3  分钟聚合 + 达成率 → CK
├── mes-production-monitor-alert/           作业4  超报/滞后/漏报    → Kafka + CK + 钉钉
├── docker/                 本地环境（MySQL / Kafka / ClickHouse / Kafka UI）
└── tools/reconcile/        对账脚本：证明"算对了"的唯一手段
```

---

## 快速开始

### 1. 起环境

```powershell
cd docker
docker compose up -d
docker compose ps
```

| 服务 | 地址 | 账号 |
|---|---|---|
| MySQL | `localhost:3306` | `mes_monitor` / `mesMonitor123456`（CDC 用 `cdc` / `cdc123456`） |
| Kafka | `localhost:9092` | — |
| Kafka UI | http://localhost:8080 | — |
| ClickHouse HTTP | `localhost:8123` | `mes_monitor` / `mesMonitor123456` |

启动后确认三件事：

```powershell
# 10 个 MES 库都建好了
docker exec mes-production-monitor-mysql mysql -uroot -proot123456 -e "SHOW DATABASES LIKE 'mes\_%';"

# binlog 是 ROW + FULL（binlog_row_image 不是 FULL 的话拿不到 UPDATE 的旧值）
docker exec mes-production-monitor-mysql mysql -uroot -proot123456 -e "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image');"

# ClickHouse 表和去重视图都在
docker exec mes-production-monitor-clickhouse clickhouse-client -u mes_monitor --password mesMonitor123456 -q "SHOW TABLES FROM mes_production_monitor"
```

### 2. 编译

```powershell
mvn -q clean install -DskipTests
```

本地 IDEA 运行需要 Flink 依赖在 classpath 上，两种方式：

- 命令行：`mvn -Plocal ...`
- IDEA：右侧 Maven 面板勾选 `local` profile（或在 Run Configuration 里勾
  "Add dependencies with provided scope to classpath"）

### 3. 造数

```powershell
# 只造维表
java -jar mes-production-monitor-datagen/target/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode dim

# 历史回填（对账基线）
java -jar mes-production-monitor-datagen/target/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode backfill --backfill.days 7

# 持续实时（验证实时链路的唯一手段，跑起来别关）
java -jar mes-production-monitor-datagen/target/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode streaming
```

### 4. 跑作业

IDEA 里依次运行四个 main 类，配置从各模块的 `*.properties` 读，可用 `--key value` 覆盖：

| 作业 | 主类 | 配置 |
|---|---|---|
| 1 CDC | `com.mes.production.monitor.cdc.CdcToKafkaJob` | `cdc.properties` |
| 2 DWD | `com.mes.production.monitor.dwd.DwdWideTableJob` | `dwd.properties` |
| 3 DWS | `com.mes.production.monitor.dws.DwsMetricsJob` | `dws.properties` |
| 4 告警 | `com.mes.production.monitor.alert.AlertJob` | `alert.properties` |

集群提交（YARN application 模式）：

```bash
flink run-application -t yarn-application \
  -Dyarn.application.name=mes-production-monitor-dwd \
  mes-production-monitor-dwd/target/mes-production-monitor-dwd-1.0-SNAPSHOT.jar \
  --checkpoint.dir hdfs:///user/flink/checkpoints/mes-production-monitor-dwd
```

> checkpoint 路径**必须**参数化：本地 `file://`，YARN 必须 `hdfs://`，
> 写死了分布式恢复时读不到状态。

---

## 开发路线

**先竖着切通一条最窄的链路，再横向长胖。** 不要按分层顺序推进——
那样会先在造数上耗掉两周，等推进到 CDC 时环境、版本、序列化问题会一起爆出来，
最难定位的时候恰好是信息最少的时候。

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| **M0** | **最小垂直切片**：单库单表手工 insert 10 条 → CK 能查到 | 环境/版本坑全部清完 |
| M1 | Docker 环境 + 10 库 schema + 造数 `backfill` | join 后维度为空的比例 < 0.1% |
| M1.5 | 造数 `streaming` + 异常注入 | 能定向注入 6 类异常 |
| M2 | 作业1 CDC | ODS topic 有 10 库数据，标签正确 |
| M3 | 作业2 宽表 | `dim_miss_count` 接近 0 |
| M4 | 作业3 聚合 | 班次日界正确 |
| **M4.5** | **对账** | 三个粒度差异率 < 0.5% |
| M5 | 作业4 告警 | 注入的异常能被准确捕获 |
| M5.5 | 可观测性补全 | Web UI 能看到各项计数 |
| M6 | 大屏 + 面试材料 | 截图、README、口径说明 |

> **M0 和 M4.5 是两个不能跳的关卡**：M0 决定你不会在复杂逻辑里排查环境问题，
> M4.5 决定你敢不敢说「这个项目算对了」。

代码里的 TODO 按里程碑编号，`TODO(M3-2)` 就是 M3 阶段第 2 项。
全局搜 `TODO(` 能看到当前所有待办。

---

## AI 辅助的边界

本项目采用「自己写 + AI 辅助」。为保证训练效果划清边界：

| 类别 | 交给 AI | 必须自己写 |
|---|---|---|
| 脚手架、POM、Docker Compose、DDL、日志配置、JDBC 样板 | ✅ | |
| 状态设计（存什么、何时清理） | | ✅ |
| watermark / 窗口 / Timer 的时间语义 | | ✅ |
| 业务口径正确性（达成率、班次日界、changelog 处理） | | ✅ |
| 所有报错的排查过程 | | ✅ |

顺序同样重要：**先自己写一版跑通，再让 AI review**，而不是让 AI 先写、自己再读。
后者会产生「我看懂了」的错觉，面试被追问一层就露底。

---

## 对账（M4.5）

```
tools/reconcile/
├── mysql_baseline.sql      按原报表口径查 MySQL，出基线
└── clickhouse_result.sql   查 ClickHouse，出结果
```

比对三个粒度：基地×班次日、车间×产线、订单级。

两个必须注意的点：

1. **ClickHouse 侧一定走 `v_` 去重视图**。ReplacingMergeTree 只在后台 merge 时去重，
   不去重的话结果必然偏大——这是第一次对账最常见的「假差异」。
2. **扫码通道的笔数必然不一致**。基线按 LPN 分组（原报表口径），
   ClickHouse 侧 DWD 是明细粒度（刻意不聚合）。要比的是报工量，笔数只在手动报工通道可比。

第一次跑差异一定很难看，逐个归因。常见原因：changelog 走错路线、班次日界时区偏移、
CK 没去重、乱序数据没等到、扫码聚合口径不一致。**每找出一个差异就往 `TROUBLESHOOTING.md` 记一条。**
