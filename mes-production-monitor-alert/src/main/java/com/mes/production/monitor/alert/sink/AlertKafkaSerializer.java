package com.mes.production.monitor.alert.sink;

import com.mes.production.monitor.common.model.AlertRecord;
import com.mes.production.monitor.common.util.JsonUtil;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * 告警 -> Kafka {@code mes-alert} 的序列化器。
 *
 * <p><b>分区键用 {@code company:linecode:productionorder}</b>（见文档 5.1）：
 * 同一订单 + 产线的告警/升级落进同一分区，下游消费者读到的是有序序列。
 * 与 {@code dwd-mes-report} 只用 productionorder 不同——告警量远小于报工明细，
 * company 前缀带来的分区倾斜可忽略，带上 company/linecode 便于下游按基地订阅。
 *
 * <p><b>value 是普通 JSON，不是 debezium-json</b>：下游直接反序列化成 {@link AlertRecord}。
 * 升级时 alertId 不变、只更新 level 与 version，由下游/ClickHouse ReplacingMergeTree 去重，
 * 见文档 6.4.3。
 */
public class AlertKafkaSerializer implements KafkaRecordSerializationSchema<AlertRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public AlertKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(AlertRecord a, KafkaSinkContext context, Long timestamp) {
        // 维表 miss 时 company/linecode/productionorder 可能全空，退回恒非空的 alertId，
        // 避免空串 key 被 hash 到同一分区造成热点（见 DwdReportKafkaSerializer 的同款兜底）。
        String key = nvl(a.company) + ":" + nvl(a.lineCode) + ":" + nvl(a.productionOrder);
        if ("::".equals(key)) {
            key = nvl(a.alertId);
        }
        byte[] value = JsonUtil.toJson(a).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, timestamp,
                key.getBytes(StandardCharsets.UTF_8), value);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
