package com.mes.production.monitor.common.util;

import com.mes.production.monitor.common.constant.MesConstants;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

/**
 * 班次日界工具：08:30:00 ~ 次日 08:29:59 归属同一个班次日，见文档 2.4 / 6.3.1。
 *
 * <p><b>为什么不用 Flink 窗口的 offset</b>：Flink 窗口边界基于 UTC epoch 计算
 * （{@code windowStart = ts - (ts - offset) % size}），北京时间 08:30 对应 UTC 00:30，
 * 正确 offset 是 0.5h 而不是 8.5h。写成 8.5h 会让班次日整体偏移 8 小时，而且错得极其隐蔽：
 * 数据一条不少、总量也对，只有归属日期错了，不做对账根本发现不了。
 *
 * <p>本项目显式计算 shift_date 写进 DWD，下游一律 keyBy(shiftDate) 自管状态——
 * 代码可控，也比"offset 应该是 0.5h"更好解释。
 */
public final class ShiftUtil {

    /** 日界偏移的分钟数：8*60 + 30 = 510 */
    private static final long SHIFT_OFFSET_MINUTES =
            MesConstants.SHIFT_START_HOUR * 60L + MesConstants.SHIFT_START_MINUTE;

    private ShiftUtil() {
    }

    /**
     * 计算报工时间所属的班次日。
     *
     * <p>例（业务时区 +8）：
     * <ul>
     *   <li>2026-09-17 08:29:59 -&gt; 班次日 2026-09-16</li>
     *   <li>2026-09-17 08:30:00 -&gt; 班次日 2026-09-17</li>
     *   <li>2026-09-18 03:00:00 -&gt; 班次日 2026-09-17（晚班）</li>
     * </ul>
     *
     * @param epochMillis 报工时间
     * @return 班次日
     */
    public static LocalDate shiftDate(long epochMillis) {
        ZonedDateTime zdt = Instant.ofEpochMilli(epochMillis).atZone(MesConstants.BIZ_ZONE);
        return zdt.minusMinutes(SHIFT_OFFSET_MINUTES).toLocalDate();
    }

    /** 班次日的 epoch day 形式，DWD 宽表用这个存储（int 比 String 省一半空间且可比较） */
    public static int shiftDateEpochDay(long epochMillis) {
        return (int) shiftDate(epochMillis).toEpochDay();
    }

    /** 班次日的起始时刻（当日 08:30:00）的 epoch millis */
    public static long shiftStartMillis(long epochMillis) {
        LocalDate day = shiftDate(epochMillis);
        LocalDateTime start = day.atTime(MesConstants.SHIFT_START_HOUR, MesConstants.SHIFT_START_MINUTE);
        return start.atZone(MesConstants.BIZ_ZONE).toInstant().toEpochMilli();
    }

    /** 班次日的结束时刻（次日 08:30:00，左闭右开）的 epoch millis */
    public static long shiftEndMillis(long epochMillis) {
        return shiftStartMillis(epochMillis) + 24 * 60 * 60 * 1000L;
    }

    /** epochDay 还原成 LocalDate，落库时用 */
    public static LocalDate fromEpochDay(int epochDay) {
        return LocalDate.ofEpochDay(epochDay);
    }
}
