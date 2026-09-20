package com.mes.production.monitor.common.util;

import com.mes.production.monitor.common.constant.MesConstants;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 班次日界单测。
 *
 * <p>这几个断言是整个项目里最值得先写测试的地方：班次日算错 8 小时的话，
 * 数据一条不少、总量也对，只有归属日期错了，跑到对账阶段才会发现，
 * 那时候要在一堆差异里反推原因非常费劲。见文档 6.3.1。
 */
public class ShiftUtilTest {

    private static long millisOf(int year, int month, int day, int hour, int minute, int second) {
        return LocalDateTime.of(year, month, day, hour, minute, second)
                .atZone(MesConstants.BIZ_ZONE)
                .toInstant()
                .toEpochMilli();
    }

    /** 08:29:59 归属前一天 */
    @Test
    public void beforeBoundaryBelongsToPreviousDay() {
        long ts = millisOf(2026, 9, 17, 8, 29, 59);
        Assert.assertEquals(LocalDate.of(2026, 9, 16), ShiftUtil.shiftDate(ts));
    }

    /** 08:30:00 归属当天 */
    @Test
    public void atBoundaryBelongsToCurrentDay() {
        long ts = millisOf(2026, 9, 17, 8, 30, 0);
        Assert.assertEquals(LocalDate.of(2026, 9, 17), ShiftUtil.shiftDate(ts));
    }

    /** 晚班凌晨 03:00 归属前一天 */
    @Test
    public void nightShiftBelongsToPreviousDay() {
        long ts = millisOf(2026, 9, 18, 3, 0, 0);
        Assert.assertEquals(LocalDate.of(2026, 9, 17), ShiftUtil.shiftDate(ts));
    }

    /** 班次区间是左闭右开的 24 小时 */
    @Test
    public void shiftRangeIs24Hours() {
        long ts = millisOf(2026, 9, 17, 14, 0, 0);
        long start = ShiftUtil.shiftStartMillis(ts);
        long end = ShiftUtil.shiftEndMillis(ts);

        Assert.assertEquals(millisOf(2026, 9, 17, 8, 30, 0), start);
        Assert.assertEquals(millisOf(2026, 9, 18, 8, 30, 0), end);
        Assert.assertEquals(24 * 60 * 60 * 1000L, end - start);
    }

    /**
     * 如果哪天想改回用 Flink 窗口 offset，这个断言提醒你正确值是 0.5h 而不是 8.5h：
     * Flink 窗口边界基于 UTC epoch 计算，08:30(+8) 对应 UTC 00:30。
     */
    @Test
    public void flinkWindowOffsetIsHalfHourNotEightAndHalf() {
        long ts = millisOf(2026, 9, 17, 14, 0, 0);
        long size = 24 * 60 * 60 * 1000L;
        long offset = MesConstants.SHIFT_WINDOW_OFFSET_MS;

        long windowStart = ts - (ts - offset) % size;
        Assert.assertEquals(ShiftUtil.shiftStartMillis(ts), windowStart);
    }
}
