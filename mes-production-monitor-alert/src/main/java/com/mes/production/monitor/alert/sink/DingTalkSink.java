package com.mes.production.monitor.alert.sink;

import com.mes.production.monitor.common.enums.AlertLevel;
import com.mes.production.monitor.common.enums.AlertType;
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
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 钉钉机器人告警推送。
 *
 * <h3>限流 + 聚合发送，两者都要（文档 6.4.3）</h3>
 * 钉钉自定义机器人限制 <b>20 条/分钟</b>，超限后整批消息全部失败。
 * 批量告警场景（比如一个基地整体断网导致 50 条产线同时漏报）下：
 * <ul>
 *   <li><b>只限流</b>：50 条里 32 条被丢，值班人还被剩下的 18 条重复消息刷屏。</li>
 *   <li><b>限流 + 聚合</b>：同 {@code company + alertType} 的告警在缓冲窗口内合并成 1 条，
 *       "50 条漏报"变成"基地A 共 50 条产线漏报"，既不触发限流，值班人也一眼看懂。</li>
 * </ul>
 *
 * <h3>为什么是本地缓冲 + 定时 flush，而不是 Timer</h3>
 * {@code RichSinkFunction} 没有 {@code TimerService}（那是 {@code KeyedProcessFunction} 的），
 * 所以用 {@code ScheduledExecutorService} 定时 flush。缓冲、限流窗口都是本地状态，
 * 因此本 sink 必须 {@code setParallelism(1)} 才全局生效（见 {@code AlertJob}）。
 *
 * <p>推送失败只记日志、不抛异常——告警已经落了 Kafka 和 ClickHouse，通知渠道是尽力而为，
 * 不能为了发钉钉把主链路拖停（阻塞会一路反压到 source）。
 */
public class DingTalkSink extends RichSinkFunction<AlertRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DingTalkSink.class);

    private static final long WINDOW_MS = 60_000L;
    /** 单条聚合消息最多列出的明细条数，超出省略，避免消息过长 */
    private static final int MAX_ITEMS_PER_MESSAGE = 20;

    private final String webhook;
    private final int maxPerMinute;
    private final boolean enabled;
    /** 聚合缓冲 flush 间隔（毫秒） */
    private final long flushIntervalMs;

    /** 最近一分钟内的发送时间戳（限流滑动窗口） */
    private transient Deque<Long> sendTimestamps;
    private transient long droppedByRateLimit;
    /** 聚合缓冲：company|alertType -> 该组的告警列表 */
    private transient Map<String, List<AlertRecord>> buffer;
    private transient ScheduledExecutorService scheduler;

    public DingTalkSink(String webhook, int maxPerMinute, boolean enabled, long flushIntervalMs) {
        this.webhook = webhook;
        this.maxPerMinute = maxPerMinute;
        this.enabled = enabled;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.sendTimestamps = new ArrayDeque<>();
        this.buffer = new LinkedHashMap<>();
        if (!enabled) {
            LOG.info("钉钉推送已关闭（alert.dingtalk.enabled=false），告警只落 Kafka 和 ClickHouse");
            return;
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dingtalk-flush");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                flush();
            } catch (Exception e) {
                // 定时线程里的异常必须记下来，不能静默吞掉
                LOG.error("钉钉定时 flush 失败", e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(AlertRecord value, Context context) {
        if (!enabled) {
            return;
        }
        synchronized (this) {
            buffer.computeIfAbsent(groupKey(value), k -> new ArrayList<>()).add(value);
        }
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        flush();
        super.close();
    }

    /** 定时线程与 close 都可能触发，必须加锁；把缓冲换出后再逐组发送，避免边发边改。 */
    private synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        Map<String, List<AlertRecord>> toSend = buffer;
        buffer = new LinkedHashMap<>();
        for (List<AlertRecord> group : toSend.values()) {
            if (group.isEmpty()) {
                continue;
            }
            if (!tryAcquireRateLimit()) {
                continue;
            }
            post(buildMarkdown(group));
        }
    }

    /** 滑动窗口限流：超过阈值丢弃并计数，不阻塞算子。现在按「聚合消息」计数，而非逐条告警。 */
    private boolean tryAcquireRateLimit() {
        long now = System.currentTimeMillis();
        while (!sendTimestamps.isEmpty() && now - sendTimestamps.peekFirst() > WINDOW_MS) {
            sendTimestamps.pollFirst();
        }
        if (sendTimestamps.size() >= maxPerMinute) {
            droppedByRateLimit++;
            if (droppedByRateLimit % 20 == 1) {
                LOG.warn("钉钉限流丢弃聚合告警 {} 条", droppedByRateLimit);
            }
            return false;
        }
        sendTimestamps.addLast(now);
        return true;
    }

    private String groupKey(AlertRecord a) {
        return nvl(a.company) + "|" + nvl(a.alertType);
    }

    /**
     * 把同一 {@code company + alertType} 的一组告警合并成一条 markdown。
     * 标题取组内最高级别（P1 &gt; P2 &gt; P3），正文列出各产线/订单（去重、超长截断）。
     */
    private String buildMarkdown(List<AlertRecord> group) {
        AlertRecord first = group.get(0);
        String alertType = nvl(first.alertType);
        String company = nvl(first.company);
        boolean isMissingReport = AlertType.MISSING_REPORT.name().equals(alertType);

        AlertLevel highest = AlertLevel.NONE;
        List<String> items = new ArrayList<>();
        for (AlertRecord a : group) {
            AlertLevel lv = parseLevel(a.level);
            if (lv != AlertLevel.NONE && (highest == AlertLevel.NONE || lv.getSeverity() < highest.getSeverity())) {
                highest = lv;
            }
            String id = isMissingReport ? a.lineCode : a.productionOrder;
            if (id != null && !id.isEmpty() && !items.contains(id)) {
                items.add(id);
            }
        }

        int total = items.size();
        String title = "[" + highest.name() + "] " + alertType + " - " + company
                + (total > 1 ? "（共 " + total + " 条）" : "");

        String label = isMissingReport ? "产线" : "订单";
        List<String> shown = items;
        String suffix = "";
        if (items.size() > MAX_ITEMS_PER_MESSAGE) {
            shown = items.subList(0, MAX_ITEMS_PER_MESSAGE);
            suffix = " 等 " + items.size() + " 项";
        }
        String text = "#### " + title + "\n"
                + "- " + label + "：" + String.join("、", shown) + suffix + "\n";

        // 手写 JSON 避免为了一个 sink 引入额外依赖；字段里的引号要转义
        return "{\"msgtype\":\"markdown\",\"markdown\":{\"title\":\"" + escape(title)
                + "\",\"text\":\"" + escape(text) + "\"}}";
    }

    private static AlertLevel parseLevel(String s) {
        if (s == null) {
            return AlertLevel.NONE;
        }
        try {
            return AlertLevel.valueOf(s);
        } catch (IllegalArgumentException e) {
            return AlertLevel.NONE;
        }
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

    private static String nvl(String s) {
        return s == null ? "" : s;
    }
}
