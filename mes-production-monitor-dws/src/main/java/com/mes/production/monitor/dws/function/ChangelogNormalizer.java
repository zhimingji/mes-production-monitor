package com.mes.production.monitor.dws.function;

import com.mes.production.monitor.common.model.DwdReportRecord;
import com.mes.production.monitor.common.model.MinuteDelta;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * changelog 归一化：把 DWD 明细直通流翻译成带符号增量 {@link MinuteDelta}。
 *
 * <p>按 record 逻辑键（{@code company|datasource|outputtype|recordId}）分区，
 * 每个键维护 lastVersion / lastQuantity：
 * <ul>
 *   <li>快照：+qty，count +1</li>
 *   <li>修正：回撤 -旧qty（count -1）+ 下发 +新qty（count +1），净 = 新-旧</li>
 *   <li>删除：只回撤 -旧qty（count -1），不下发正向</li>
 * </ul>
 *
 * <p>下游 SummingMergeTree 只需无脑累加这些增量，修正/删除/迟到都天然正确——这是
 * "把每条记录怎么变"与"按维度怎么加"解耦的做法（见文档 6.1.1 / 6.3）。
 */
public class ChangelogNormalizer
        extends KeyedProcessFunction<String, DwdReportRecord, MinuteDelta> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Long> lastVersion;
    private transient ValueState<BigDecimal> lastQuantity;

    @Override
    public void open(Configuration parameters) {
        this.lastVersion = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-version", Long.class));
        this.lastQuantity = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-quantity", BigDecimal.class));
    }

    @Override
    public void processElement(DwdReportRecord r, Context ctx, Collector<MinuteDelta> out) throws Exception {
        Long prevVersion = lastVersion.value();
        if (prevVersion != null && r.version <= prevVersion) {
            // 更旧或重复的版本（at-least-once 重放 / 乱序），忽略
            return;
        }

        // 回撤上一次累计的量（若之前累计过）
        // prevQty.negate(): 上一次累计的量取反 → -prevQty
        BigDecimal prevQty = lastQuantity.value();
        if (prevQty != null && prevQty.signum() != 0) {
            emit(r, prevQty.negate(), -1L, out);
        }

        // 应用本次
        if (r.isDeleted) {
            lastQuantity.update(BigDecimal.ZERO);
        } else {
            BigDecimal qty = r.lotQuantity == null ? BigDecimal.ZERO : r.lotQuantity;
            emit(r, qty, 1L, out);
            lastQuantity.update(qty);
        }
        lastVersion.update(r.version);
    }

    private void emit(DwdReportRecord r, BigDecimal quantity, long count, Collector<MinuteDelta> out) {
        MinuteDelta d = new MinuteDelta();
        d.company = r.company;
        d.workshop = r.workshop;
        d.lineCode = r.lineCode;
        d.lineName = r.lineName;
        d.partCode = r.partCode;
        d.quantity = quantity;
        d.count = count;
        d.eventTime = r.outputDateTime;
        out.collect(d);
    }
}
