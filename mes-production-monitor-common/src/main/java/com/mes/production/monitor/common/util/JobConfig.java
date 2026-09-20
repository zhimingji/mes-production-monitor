package com.mes.production.monitor.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 作业配置加载：classpath properties 打底，命令行参数覆盖。
 *
 * <p>所有连接串、阈值、并行度、checkpoint 路径都必须走这里，不要写死在代码里。
 * 尤其是 checkpoint 路径——本地 local mode 用 {@code file://}，YARN 必须用 {@code hdfs://}，
 * 写死了分布式恢复时状态读不到，见文档 12 节坑 #7。
 *
 * <p>用法：
 * <pre>
 * // 命令行：--kafka.bootstrap.servers localhost:9092 --checkpoint.dir hdfs:///flink/ck
 * JobConfig conf = JobConfig.load("dwd.properties", args);
 * String brokers = conf.getString("kafka.bootstrap.servers");
 * </pre>
 */
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Properties props = new Properties();

    private JobConfig() {
    }

    /**
     * @param resourceName classpath 下的 properties 文件名，如 {@code dwd.properties}
     * @param args         main 方法参数，形如 {@code --key value}，覆盖文件里的同名配置
     */
    public static JobConfig load(String resourceName, String[] args) {
        JobConfig conf = new JobConfig();
        conf.loadFromClasspath(resourceName);
        conf.overrideFromArgs(args);
        return conf;
    }

    private void loadFromClasspath(String resourceName) {
        try (InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resourceName)) {
            if (in != null) {
                props.load(in);
                return;
            }
        } catch (IOException e) {
            throw new IllegalStateException("读取配置文件失败: " + resourceName, e);
        }
        // classpath 里没有时尝试当作外部路径读（方便 YARN 提交时用 -yt 分发的配置）
        if (Files.exists(Paths.get(resourceName))) {
            try (InputStream in = Files.newInputStream(Paths.get(resourceName))) {
                props.load(in);
                return;
            } catch (IOException e) {
                throw new IllegalStateException("读取配置文件失败: " + resourceName, e);
            }
        }
        throw new IllegalStateException("找不到配置文件: " + resourceName);
    }

    private void overrideFromArgs(String[] args) {
        if (args == null) {
            return;
        }
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                props.setProperty(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
    }

    public String getString(String key) {
        String v = props.getProperty(key);
        if (v == null) {
            throw new IllegalArgumentException("缺少必填配置项: " + key);
        }
        return v.trim();
    }

    public String getString(String key, String defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : v.trim();
    }

    public int getInt(String key, int defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : Integer.parseInt(v.trim());
    }

    public long getLong(String key, long defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : Long.parseLong(v.trim());
    }

    public double getDouble(String key, double defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : Double.parseDouble(v.trim());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v.trim());
    }

    /** 返回副本，用于构造 KafkaSource/KafkaSink 的 Properties */
    public Properties toProperties() {
        Properties copy = new Properties();
        copy.putAll(props);
        return copy;
    }
}
