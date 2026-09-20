# 踩坑记录

> **这份文件是你的面试弹药库，不是可选项。**
>
> 每个坑记四段：**现象 / 报错原文 / 根因 / 解决**。别嫌麻烦——三个月后面试官问
> 「讲一个你印象最深的技术难题」，你翻这个文件就有十个真实的可讲，
> 而且细节具体到报错原文，可信度是编不出来的。
>
> 记录格式看下面的模板。带 `[预置]` 标记的是设计阶段已经识别、但**你仍应亲手踩一遍**的坑，
> 踩完把「现象」那一栏换成你自己看到的真实输出。

---

## 模板

```
### N. 一句话标题

- 阶段：M2 / M3 / ...
- 现象：
- 报错原文：
- 排查过程：
- 根因：
- 解决：
- 面试可讲点：
```

---

## [预置] 1. Flink 与 Flink CDC 版本不兼容

- 阶段：M0
- 根因：Flink CDC 2.4.x 官方仅支持 Flink 1.13 ~ **1.17**，与 Flink 1.18 不兼容。
  另外 `flink-connector-kafka` 自 Flink 1.17 起已外置，**不存在 `1.18.1` 这个版本号**，
  1.18 对应的是 `3.0.2-1.18`。
- 解决：Flink 1.18.1 + Flink CDC **3.2.1**（groupId 已改为 `org.apache.flink`）。
  保守方案是 Flink 1.17.2 + CDC 2.4.2。见 `技术设计文档.md` 4.1。
- 面试可讲点：connector 外置后的版本号规则（`{connector版本}-{flink版本}`）、
  为什么用 `flink-sql-connector-mysql-cdc`（已 shade）而不是 `flink-connector-mysql-cdc`
  （需要自己对齐 `flink-shaded-guava`）。

## [预置] 2. changelog 流写 append-only Kafka sink 失败

- 阶段：M2
- 报错原文：`TableException: Table sink 'xxx' doesn't support consuming update changes which is produced by node TableSourceScan(...)`
- 根因：`lot` 有 UPDATE（报工修正），CDC 产出 changelog 流；普通 `kafka` connector 是 append-only sink。
- 解决：用 `kafka` + `debezium-json` format 保留完整 changelog。见文档 6.1.1。
- **真正危险的不是这个报错**，而是绕过报错的错误做法：用普通 `json` format 只取新值。
  那样表面能跑、Kafka 里有数据、一切正常，但每次报工修正都会重复累加，
  **只有做对账才发现得了**。记录你对账时的差异数字。

## [预置] 3. MySQL CDC server-id 冲突

- 阶段：M2
- 报错原文：`A slave with the same server_uuid/server_id as this slave has connected to the master`
- 根因：每个 CDC source 的每个并行子任务都要占一个 server-id。多个 source 或多并行度时区间重叠。
- 解决：配互不重叠的区间，区间长度 ≥ 并行度。本项目 `5400-5409` / `5410-5419`。

## [预置] 4. 维表广播初始化竞态导致维度全空

- 阶段：M3
- 现象：数据条数一条不少，但 `partname` / `linename` / `workshop` 全是空。
  小数据量时可能碰巧不出现，一放量或一重启就出现。
- 根因：广播流（维表）还没到，主数据流已经进来了，读广播状态是空的。
- 解决：算子 `open()` 里先用 JDBC 全量 bootstrap 兜底，广播流只负责后续增量刷新。
  同时给维度 miss 加 Counter metric——维度 join 不上是**静默失败**，不监控发现不了。见文档 6.2.1。

## [预置] 5. 漏报检测用事件时间 Timer 永远不触发

- 阶段：M5
- 现象：造数器把产线断流 90 分钟，`MISSING_REPORT` 告警**一条都没有**。
- 根因：产线停了 → 该分区没有新数据 → watermark 不推进 → 事件时间不前进 → Timer 永不触发。
  **恰恰在最需要告警的时候，告警发不出来。**
- 解决：改用处理时间 Timer。`withIdleness` 解决不了这个问题——它只让空闲分区不拖累其他分区，
  全部分区都没数据时 watermark 依然不动。见文档 6.4.2。
- 面试可讲点：这是流计算里区分度最高的一道题之一。事件时间是**由数据驱动**的，
  没有数据就没有时间；「多久没来数据」本质属于处理时间语义。
  **务必先用事件时间实现、亲眼看到告警没出来，再改。** 把你观察到的现象记在这里。

## [预置] 6. ClickHouse Too many parts

- 阶段：M3
- 报错原文：`DB::Exception: Too many parts (N). Merges are processing significantly slower than inserts`
- 根因：CK 每次 INSERT 生成一个 data part，Flink 高频小批量写入让 part 数量迅速累积。
- 解决：`ClickHouseBatchSink` 做条数 + 时间双触发攒批，并在 `snapshotState` 强制 flush。
  单一触发条件都不行：只按条数低峰期不落地，只按时间高峰期批次过大。见文档 11.2。

## [预置] 7. ReplacingMergeTree 查出重复行

- 阶段：M4.5（对账）
- 现象：对账时 ClickHouse 侧数量明显大于 MySQL 基线。
- 根因：ReplacingMergeTree **只在后台 merge 时去重，merge 时机不确定**，
  查询不加 `FINAL` / `argMax` 就会看到多个版本。
- 解决：下游和对账一律走 `v_` 去重视图。用 `tools/reconcile/clickhouse_result.sql`
  最后那段「自查」查询能看到哪些 `record_id` 有多版本。见文档 5.3。

## [预置] 8. 班次日界偏移 8 小时

- 阶段：M4
- 现象：数据一条不少、产量总数也对，只有**归属日期**错了。极其隐蔽。
- 根因：Flink 窗口边界基于 UTC epoch 计算。北京时间 08:30 对应 UTC 00:30，
  正确 offset 是 **0.5h**（= 8.5h − 8h 时区偏移），不是 8.5h。
- 解决：本项目不依赖窗口 offset，用 `ShiftUtil` 显式计算班次日。
  `ShiftUtilTest` 里有断言锁住这个语义。见文档 6.3.1。

## [预置] 9. POJO 静默退化成 Kryo

- 阶段：M3
- 现象：没有报错，但吞吐明显偏低。
- 根因：类不满足 Flink POJO 规范（public 类 + public 无参构造 + public 字段或标准 getter/setter），
  Flink 静默改用 Kryo，性能差好几倍。
- 解决：`env.getConfig().disableGenericTypes()`（本项目在 `FlinkEnvFactory` 里由
  `job.disable.generic.types` 控制，默认开），一旦有类型走 Kryo 直接抛异常。见文档 6.2.3。

## [预置] 10. ClickHouse String 列不接受 NULL

- 阶段：M3
- 报错原文：`Cannot convert NULL to non-Nullable type` 一类
- 根因：CK 的 `String` 默认不可为 NULL。维表 join miss 时维度字段是 `null`，直接 setString 就炸。
- 解决：所有可能为空的字段落成空串（各 sink 里的 `nvl()`）。
  注意 `recover_time` 是 `Nullable(DateTime)`，未恢复时必须写 NULL 而不是 0，
  否则大屏上会出现 1970 年。

---

## 你自己踩到的坑从这里开始记

### 11.
