package com.mes.production.monitor.common.flink;

import com.mes.production.monitor.common.util.JobConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
 */
public final class FlinkEnvFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvFactory.class);

    private FlinkEnvFactory() {
    }

    public static StreamExecutionEnvironment create(JobConfig conf) {
        Configuration flinkConf = new Configuration();

        // 状态后端用配置项设置而不是 new EmbeddedRocksDBStateBackend()，
        // 这样 common 模块不必依赖 flink-statebackend-rocksdb。
        String backend = conf.getString("state.backend.type", "rocksdb");
        flinkConf.setString("state.backend.type", backend);
        flinkConf.setString("state.backend.incremental",
                String.valueOf(conf.getBoolean("state.backend.incremental", true)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);

        env.setParallelism(conf.getInt("job.parallelism", 2));

        // ---------- checkpoint ----------
        long interval = conf.getLong("checkpoint.interval.ms", 60_000L);
        env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig ckConf = env.getCheckpointConfig();
        ckConf.setCheckpointStorage(conf.getString("checkpoint.dir"));
        // 两次 checkpoint 之间至少留出的间隔，防止 checkpoint 挤占正常处理时间
        ckConf.setMinPauseBetweenCheckpoints(conf.getLong("checkpoint.min.pause.ms", 30_000L));
        ckConf.setCheckpointTimeout(conf.getLong("checkpoint.timeout.ms", 300_000L));
        ckConf.setMaxConcurrentCheckpoints(1);
        ckConf.setTolerableCheckpointFailureNumber(conf.getInt("checkpoint.tolerable.failures", 3));
        // 手动 cancel 作业后保留 checkpoint，方便从上次位置恢复继续调试
        ckConf.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---------- 重启策略 ----------
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                conf.getInt("restart.attempts", 3),
                conf.getLong("restart.delay.ms", 10_000L)));

        // ---------- 类型检查 ----------
        // 开发期强制打开：POJO 不满足规范会静默退化成 Kryo，性能差好几倍且不报错。
        // 打开后一旦有类型走 Kryo 会直接抛异常，把问题暴露在调试阶段。见文档 6.2.3。
        if (conf.getBoolean("job.disable.generic.types", true)) {
            env.getConfig().disableGenericTypes();
        }

        LOG.info("Flink env 就绪: parallelism={}, stateBackend={}, ckInterval={}ms, ckDir={}",
                env.getParallelism(), backend, interval, conf.getString("checkpoint.dir"));
        return env;
    }
}
