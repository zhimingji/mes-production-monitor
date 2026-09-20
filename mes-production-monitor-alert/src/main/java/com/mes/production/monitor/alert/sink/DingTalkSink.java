package com.mes.production.monitor.alert.sink;

import com.mes.production.monitor.common.model.AlertRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 钉钉机器人告警推送。
 *
 * <h3>限流是必须的，不是可选的</h3>
 * 钉钉自定义机器人限制 <b>20 条/分钟</b>，超限后整批消息全部失败。
 * 批量告警场景（比如一个基地整体断网导致 50 条产线同时漏报）下，不限流的结果是一条都发不出去。
 *
 * <p>本类实现了滑动窗口限流：超过阈值时丢弃并计数，而不是阻塞算子——
 * sink 阻塞会一路反压到 source，为了发钉钉把整个数据链路拖停是不划算的。
 *
 * <p>更好的做法是聚合发送（同基地同类型的多条合并成一条），见 {@code TODO(M5-4)}。
 */
public class DingTalkSink extends RichSinkFunction<AlertRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DingTalkSink.class);

    private static final long WINDOW_MS = 60_000L;

    private final String webhook;
    private final int maxPerMinute;
    private final boolean enabled;

    /** 最近一分钟内的发送时间戳 */
    private transient Deque<Long> sendTimestamps;
    private transient long droppedByRateLimit;

    public DingTalkSink(String webhook, int maxPerMinute, boolean enabled) {
        this.webhook = webhook;
        this.maxPerMinute = maxPerMinute;
        this.enabled = enabled;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.sendTimestamps = new ArrayDeque<>();
        if (!enabled) {
            LOG.info("钉钉推送已关闭（alert.dingtalk.enabled=false），告警只落 Kafka 和 ClickHouse");
        }
    }

    @Override
    public void invoke(AlertRecord value, Context context) throws Exception {
        if (!enabled) {
            return;
        }
        long now = System.currentTimeMillis();
        while (!sendTimestamps.isEmpty() && now - sendTimestamps.peekFirst() > WINDOW_MS) {
            sendTimestamps.pollFirst();
        }
        if (sendTimestamps.size() >= maxPerMinute) {
            droppedByRateLimit++;
            if (droppedByRateLimit % 20 == 1) {
                LOG.warn("钉钉限流丢弃告警 {} 条，最近一条: type={}, company={}, line={}",
                        droppedByRateLimit, value.alertType, value.company, value.lineCode);
            }
            return;
        }
        sendTimestamps.addLast(now);
        post(buildMarkdown(value));
    }

    /**
     * TODO(M5-4) 聚合发送：同 company + alertType 的多条告警合并成一条消息，
     * 用处理时间 Timer 或本地缓冲 + 定时 flush 实现。这样 50 条漏报变成 1 条消息，
     * 既不触发限流，值班人也更容易读。
     */
    private String buildMarkdown(AlertRecord a) {
        String title = "[" + a.level + "] " + a.alertType + " - " + a.company;
        String text = "#### " + title + "\n"
                + "- 产线：" + a.lineCode + "\n"
                + "- 订单：" + a.productionOrder + "\n"
                + "- 计划/实际：" + a.orderQuantity + " / " + a.reportQuantity + "\n"
                + "- 达成率：" + a.achieveRate + "\n";
        // 手写 JSON 避免为了一个 sink 引入额外依赖；字段里的引号要转义
        return "{\"msgtype\":\"markdown\",\"markdown\":{\"title\":\"" + escape(title)
                + "\",\"text\":\"" + escape(text) + "\"}}";
    }

    private void post(String body) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(webhook).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            int code = conn.getResponseCode();
            if (code != 200) {
                LOG.warn("钉钉推送失败, httpCode={}", code);
            }
        } catch (Exception e) {
            // 推送失败不能影响主链路：告警已经落了 Kafka 和 ClickHouse，通知渠道是尽力而为
            LOG.warn("钉钉推送异常，已忽略", e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
