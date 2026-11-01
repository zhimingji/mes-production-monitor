package com.mes.production.monitor.dwd.sink;

import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.util.JsonUtil;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * DWD -&gt; Kafka {@code dwd-mes-report} 的序列化器。
 *
 * <p><b>分区键用 productionorder（订单编号）</b>：同一订单的明细进同一分区，DWS 按订单聚合时能局部化。
 * 不用 {@code "company:datasource:productionorder"} 这种带前缀的 key——
 * company 前缀会让分区分布跟着基地规模倾斜（文档 11.1）。
 *
 * <p><b>value 是普通 JSON，不是 debezium-json</b>：DWD 已在 processElement 里把 changelog
 * 消化成"最新值"，下游 DWS/告警读到的是干净宽表。update 的去重靠 recordId + version，
 * 由下游/ClickHouse ReplacingMergeTree 承担，见文档 6.1.1 / 6.2。
 *
 * <p><b>{@code isDeleted} 字段</b>：物理删除（op=d）会产出 {@code isDeleted=true} 的墓碑，
 * 随 JSON 一起下发（普通记录恒为 {@code false}）。DWS 消费时须据此做回撤，见文档 6.1.1。
 */
public class DwdReportKafkaSerializer implements KafkaRecordSerializationSchema<DwdReportRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public DwdReportKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DwdReportRecord r, KafkaSinkContext context, Long timestamp) {
        // 订单号缺失（订单关联 miss）或为空串时退回 recordId：保证 key 非空，
        // 空串 key 会被 hash 到同一分区造成热点，所以空串也要兜底
        String key = (r.productionOrder == null || r.productionOrder.isEmpty()) ? r.recordId : r.productionOrder;
        byte[] value = JsonUtil.toJson(r).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(topic, null, timestamp,
                key.getBytes(StandardCharsets.UTF_8), value);
    }
}
