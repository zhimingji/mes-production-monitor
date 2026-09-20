package com.mes.production.monitor.dwd.dim;

import com.mes.production.monitor.common.model.DimTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 维表 JDBC 全量加载。
 *
 * <p><b>为什么需要它</b>：Broadcast State 有初始化竞态——广播流还没到，主数据流已经进来了，
 * 此时读广播状态是空的，join 出来维度全是 null。更麻烦的是数据量小时可能碰巧不出现，
 * 一放量或一重启就出现，容易误判成 join 逻辑写错。
 *
 * <p>所以算子 {@code open()} 里先用本类做一次全量 bootstrap 兜底，广播流只负责后续增量刷新。
 * 见文档 6.2.1。
 */
public final class DimLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DimLoader.class);

    private static final String[] COMPANIES = {"zh", "zz", "hf", "wh", "cq"};
    private static final String[] PREFIXES = {"mes_old_", "mes_new_"};

    private DimLoader() {
    }

    /** 10 个库的库名清单 */
    public static List<String> allDatabases() {
        List<String> dbs = new ArrayList<>(COMPANIES.length * PREFIXES.length);
        for (String company : COMPANIES) {
            for (String prefix : PREFIXES) {
                dbs.add(prefix + company);
            }
        }
        return dbs;
    }

    /**
     * 加载全部 10 个库的维表。
     *
     * @param jdbcUrlTemplate 形如 {@code jdbc:mysql://localhost:3306/%s?useSSL=false&serverTimezone=Asia/Shanghai}，
     *                        {@code %s} 会被库名替换
     */
    public static DimCache loadAll(String jdbcUrlTemplate, String username, String password) {
        DimCache cache = new DimCache();
        for (String db : allDatabases()) {
            String url = String.format(jdbcUrlTemplate, db);
            try (Connection conn = DriverManager.getConnection(url, username, password)) {
                loadOneDatabase(conn, db, cache);
            } catch (SQLException e) {
                // 单库失败不应让整个作业起不来，但必须显式告警：
                // 少一个库的维表 = 该基地全部数据维度为空，会体现在 dim_miss_count 上
                LOG.error("加载维表失败, db={}，该基地数据的维度将为空", db, e);
            }
        }
        cache.loadedAt = System.currentTimeMillis();
        LOG.info("维表加载完成: {}", cache.summary());
        return cache;
    }

    private static void loadOneDatabase(Connection conn, String db, DimCache cache) throws SQLException {
        cache.ordersById.put(db, new HashMap<>());
        cache.parts.put(db, new HashMap<>());
        cache.workCenters.put(db, new HashMap<>());
        cache.workSites.put(db, new HashMap<>());
        cache.users.put(db, new HashMap<>());

        try (Statement st = conn.createStatement()) {
            // ---------- productionorder ----------
            try (ResultSet rs = st.executeQuery(
                    "SELECT productionorderid, productionorderno, orderquantity, worksiteid, partid, "
                            + "createddate, duedate FROM productionorder")) {
                while (rs.next()) {
                    DimTables.ProductionOrder o = new DimTables.ProductionOrder();
                    o.productionOrderId = rs.getLong("productionorderid");
                    o.productionOrderNo = rs.getString("productionorderno");
                    o.orderQuantity = rs.getLong("orderquantity");
                    o.worksiteId = rs.getLong("worksiteid");
                    o.partId = rs.getLong("partid");
                    o.createdDate = toMillis(rs.getTimestamp("createddate"));
                    o.dueDate = toMillis(rs.getTimestamp("duedate"));
                    cache.ordersById.get(db).put(o.productionOrderId, o);
                }
            }

            // ---------- part ----------
            try (ResultSet rs = st.executeQuery("SELECT partid, partcode, partname FROM part")) {
                while (rs.next()) {
                    DimTables.Part p = new DimTables.Part();
                    p.partId = rs.getLong("partid");
                    p.partCode = rs.getString("partcode");
                    p.partName = rs.getString("partname");
                    cache.parts.get(db).put(p.partId, p);
                }
            }

            // ---------- workcenter ----------
            try (ResultSet rs = st.executeQuery(
                    "SELECT workcenterid, workcentername, description, worksiteid, workcentertype FROM workcenter")) {
                while (rs.next()) {
                    DimTables.WorkCenter w = new DimTables.WorkCenter();
                    w.workCenterId = rs.getLong("workcenterid");
                    w.workCenterName = rs.getString("workcentername");
                    w.description = rs.getString("description");
                    w.worksiteId = rs.getLong("worksiteid");
                    w.workCenterType = rs.getString("workcentertype");
                    cache.workCenters.get(db).put(w.workCenterId, w);
                }
            }

            // ---------- worksite ----------
            try (ResultSet rs = st.executeQuery("SELECT worksiteid, description FROM worksite")) {
                while (rs.next()) {
                    DimTables.WorkSite s = new DimTables.WorkSite();
                    s.worksiteId = rs.getLong("worksiteid");
                    s.description = rs.getString("description");
                    cache.workSites.get(db).put(s.worksiteId, s);
                }
            }

            // ---------- user ----------
            try (ResultSet rs = st.executeQuery("SELECT userid, username FROM `user`")) {
                while (rs.next()) {
                    DimTables.User u = new DimTables.User();
                    u.userId = rs.getLong("userid");
                    u.userName = rs.getString("username");
                    cache.users.get(db).put(u.userId, u);
                }
            }
        }
    }

    private static long toMillis(Timestamp ts) {
        return ts == null ? 0L : ts.getTime();
    }
}
