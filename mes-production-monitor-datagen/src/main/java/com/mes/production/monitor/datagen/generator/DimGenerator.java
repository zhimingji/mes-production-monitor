package com.mes.production.monitor.datagen.generator;

import com.mes.production.monitor.datagen.writer.MysqlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 维表造数：worksite / workcenter / part / user。
 *
 * <p><b>引用完整性顺序（文档 9.3.1）</b>：
 * worksite → workcenter → part → user → productionorder → lot / barcodeautomatic。
 * 外键必须指向真实存在的记录，否则 join 后维度大面积为空，
 * 会被误判成 DWD 的广播竞态 bug，白排查半天。
 */
public class DimGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DimGenerator.class);

    /** 参照真实压缩机厂的分厂划分 */
    private static final String[] WORKSHOPS = {
            "涡旋分厂", "总装分厂", "泵体分厂", "注塑分厂", "试制分厂", "电机分厂", "冲压分厂"
    };

    /** 产线类型。总装线是扫码通道的主力（文档 9.3 提到扫码集中在总装线） */
    private static final String[] WORKCENTER_TYPES = {
            "总装", "涡旋", "泵体", "注塑", "电机", "冲压"
    };

    private final MysqlWriter writer;
    private final int workCentersPerSite;
    private final int partCount;
    private final int userCount;

    public DimGenerator(MysqlWriter writer, int workCentersPerSite, int partCount, int userCount) {
        this.writer = writer;
        this.workCentersPerSite = workCentersPerSite;
        this.partCount = partCount;
        this.userCount = userCount;
    }

    public void generate(String db, String company) {
        generateWorkSites(db);
        generateWorkCenters(db, company);
        generateParts(db);
        generateUsers(db, company);
        LOG.info("[{}] 维表造数完成: worksite={}, workcenter={}, part={}, user={}",
                db, WORKSHOPS.length, WORKSHOPS.length * workCentersPerSite, partCount, userCount);
    }

    private void generateWorkSites(String db) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < WORKSHOPS.length; i++) {
            rows.add(new Object[]{(long) (i + 1), WORKSHOPS[i]});
        }
        writer.batch(db,
                "INSERT IGNORE INTO worksite (worksiteid, description) VALUES (?, ?)",
                rows);
    }

    private void generateWorkCenters(String db, String company) {
        List<Object[]> rows = new ArrayList<>();
        long id = 1;
        String prefix = company.toUpperCase();
        for (int siteIdx = 0; siteIdx < WORKSHOPS.length; siteIdx++) {
            long worksiteId = siteIdx + 1;
            String type = WORKCENTER_TYPES[siteIdx % WORKCENTER_TYPES.length];
            for (int k = 1; k <= workCentersPerSite; k++) {
                // 形如 ZH01Z01：基地两位缩写 + 车间号 + 线号，模仿真实产线编码
                String code = String.format("%s%02dZ%02d", prefix, worksiteId, k);
                String name = type + (char) ('A' + (k - 1) % 26) + "线";
                rows.add(new Object[]{id++, code, name, worksiteId, type});
            }
        }
        writer.batch(db,
                "INSERT IGNORE INTO workcenter "
                        + "(workcenterid, workcentername, description, worksiteid, workcentertype) "
                        + "VALUES (?, ?, ?, ?, ?)",
                rows);
    }

    private void generateParts(String db) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 1; i <= partCount; i++) {
            rows.add(new Object[]{
                    (long) i,
                    String.format("PN%06d", i),
                    "压缩机部件-" + i
            });
        }
        writer.batch(db,
                "INSERT IGNORE INTO part (partid, partcode, partname) VALUES (?, ?, ?)",
                rows);
    }

    private void generateUsers(String db, String company) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 1; i <= userCount; i++) {
            rows.add(new Object[]{(long) i, company + "_op" + String.format("%03d", i)});
        }
        writer.batch(db,
                "INSERT IGNORE INTO `user` (userid, username) VALUES (?, ?)",
                rows);
    }

    public int workCenterCount() {
        return WORKSHOPS.length * workCentersPerSite;
    }

    public int workSiteCount() {
        return WORKSHOPS.length;
    }
}
