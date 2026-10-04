package com.mes.production.monitor.common.flink;

import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一构建 StreamExecutionEnvironment。四个作业都走这里，保证 checkpoint / 状态后端 /
 * 重启策略一致，也保证所有参数都是配置化的而不是写死的。
 *
 * <p><b>checkpoint 路径必须参数化</b>：本地 local mode 用 {@code file:///...}，
 * YARN 集群必须用 {@code hdfs://...}（或 {@code oss://...}）。写死了分布式恢复时读不到状态，
 * 见文档 12 节坑 #7。
 *
 * <p><b>barrier 对齐方式保持默认「对齐 checkpoint」</b>：通用作业吞吐不高、对齐背压风险小；
 * 非对齐 checkpoint 状态更大，且只支持 EXACTLY_ONCE，会和 {@code checkpoint.mode=at-least-once} 冲突。
 * 真正需要非对齐的作业（如 CDC 采集）在各自 job 里单独 {@code enableUnalignedCheckpoints()}，不要在这里一刀切
 *
 * <p>参数配置项（checkpoint 的 key 与 CDC 采集作业 {@code CdcToKafkaJob} 保持一致）：
 * <ul>
 *   <li>状态后端：{@code state.backend.type}（默认 rocksdb）、{@code state.backend.incremental}（默认 true）</li>
 *   <li>并行度：{@code job.parallelism}（默认 2）</li>
 *   <li>checkpoint：{@code checkpoint.interval.ms}（默认 60s）、{@code checkpoint.dir}（必填）、
 *       {@code checkpoint.mode}（默认 exactly-once）、{@code checkpoint.timeout.ms}（默认 60s）、
 *       {@code checkpoint.failure.number.tolerable}（默认 10）</li>
 *   <li>重启策略：{@code restart.strategy}（fixed-delay 默认 / failure-rate / none）；
 *       fixed-delay：{@code restart.attempts}（3）、{@code restart.delay.ms}（10s）；
 *       failure-rate：{@code restart.failure.rate.max}（20）、{@code restart.failure.rate.interval.ms}（1 天）、{@code restart.failure.rate.delay.ms}（1 分钟）</li>
 *   <li>类型检查：{@code job.disable.generic.types}（默认 true）</li>
 * </ul>
 */
public final class FlinkEnvFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvFactory.class);

    private FlinkEnvFactory() {
    }

    public static StreamExecutionEnvironment create(JobConfig conf) {
        Configuration flinkConf = new Configuration();

        // 状态后端用配置项设置而不是 new EmbeddedRocksDBStateBackend()
        // 这样 common 模块不必依赖 flink-statebackend-rocksdb
        String backendType = conf.getString("state.backend.type", "rocksdb");
        flinkConf.setString("state.backend.type", backendType);
        String backendIncremental = String.valueOf(conf.getBoolean("state.backend.incremental", true));
        flinkConf.setString("state.backend.incremental", backendIncremental);

        // Job 并行度，默认并行度为2
        Integer parallelism = conf.getInt("job.parallelism", 2);

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);

        env.setParallelism(parallelism);

        // ---------- checkpoint ----------
        // 检查点间隔周期可配置：默认1分钟
        long checkpointInterval = conf.getLong("checkpoint.interval.ms", 60_000L);
        // 检查点一致性语义可配置：默认 exactly-once，可配置 at-least-once
        CheckpointingMode checkpointMode = parseCheckpointingMode(conf.getString("checkpoint.mode", "exactly-once"));
        env.enableCheckpointing(checkpointInterval);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 检查点目录（必须配置）
        checkpointConfig.setCheckpointStorage(conf.getString("checkpoint.dir"));
        checkpointConfig.setCheckpointingMode(checkpointMode);
        // 检查点超时时长可配置：默认1分钟
        checkpointConfig.setCheckpointTimeout(conf.getLong("checkpoint.timeout.ms", 60_000L));
        // 允许失败次数可配置：默认 10 次
        checkpointConfig.setTolerableCheckpointFailureNumber(conf.getInt("checkpoint.failure.number.tolerable", 10));
        // 手动 cancel 作业后保留 checkpoint（RETAIN_ON_CANCELLATION），方便从上次位置恢复继续调试
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---------- 重启策略（类型可配置：fixed-delay / failure-rate / none） ----------
        env.setRestartStrategy(parseRestartStrategy(conf));

        // ---------- 类型检查 ----------
        // 开发期强制打开：POJO 不满足规范会静默退化成 Kryo，性能差好几倍且不报错
        // 打开后一旦有类型走 Kryo 会直接抛异常，把问题暴露在调试阶段
        if (conf.getBoolean("job.disable.generic.types", true)) {
            env.getConfig().disableGenericTypes();
        }

        LOG.info("Flink env 就绪: parallelism={}, stateBackend={}, ckMode={}, ckInterval={}ms, ckDir={}",
                env.getParallelism(), backendType, checkpointMode, checkpointInterval, conf.getString("checkpoint.dir"));

        return env;
    }

    /**
     * 把配置字符串解析成 {@link CheckpointingMode}（连字符写法，与 Flink 自身配置一致）。
     * 非法值直接抛异常，避免静默回退到默认语义造成线上脏数据。
     */
    private static CheckpointingMode parseCheckpointingMode(String mode) {
        switch (mode.trim().toLowerCase()) {
            case "exactly-once":
                return CheckpointingMode.EXACTLY_ONCE;
            case "at-least-once":
                return CheckpointingMode.AT_LEAST_ONCE;
            default:
                throw new IllegalArgumentException(
                        "未知的 checkpoint.mode: " + mode + "（支持 exactly-once / at-least-once）");
        }
    }

    /**
     * 按 {@code restart.strategy} 选择重启策略：
     * <ul>
     *   <li>{@code fixed-delay}（默认）：{@code restart.attempts} + {@code restart.delay.ms}</li>
     *   <li>{@code failure-rate}：{@code restart.failure.rate.max} + {@code .interval.ms} + {@code .delay.ms}</li>
     *   <li>{@code none}：不自动重启</li>
     * </ul>
     */
    private static RestartStrategies.RestartStrategyConfiguration parseRestartStrategy(JobConfig conf) {
        String strategy = conf.getString("restart.strategy", "fixed-delay").trim().toLowerCase();
        switch (strategy) {
            case "none":
                return RestartStrategies.noRestart();
            case "failure-rate":
                return RestartStrategies.failureRateRestart(
                        conf.getInt("restart.failure.rate.max", 20),
                        Time.milliseconds(conf.getLong("restart.failure.rate.interval.ms", 86_400_000L)),
                        Time.milliseconds(conf.getLong("restart.failure.rate.delay.ms", 60_000L)));
            case "fixed-delay":
                return RestartStrategies.fixedDelayRestart(
                        conf.getInt("restart.attempts", 3),
                        conf.getLong("restart.delay.ms", 10_000L));
            default:
                throw new IllegalArgumentException(
                        "未知的 restart.strategy: " + strategy + "（支持 fixed-delay / failure-rate / none）");
        }
    }
}
