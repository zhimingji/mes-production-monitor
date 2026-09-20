package com.mes.production.monitor.dwd.dim;

import com.mes.production.monitor.common.model.DimTables;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 维表内存缓存。按源库名分组（10 个库各有一套维表，id 只在库内唯一，不能混着放）。
 *
 * <p>数据量：每基地约 300 订单 + 50 产线 + 若干物料/用户，10 库合计万级以内，
 * 完全放得进内存，这也是选 Broadcast State 而不是 Lookup Join 的前提。
 */
public class DimCache implements Serializable {

    private static final long serialVersionUID = 1L;

    /** dbName -> (productionorderid -> 订单) */
    public Map<String, Map<Long, DimTables.ProductionOrder>> ordersById = new HashMap<>();
    /** dbName -> (partid -> 物料) */
    public Map<String, Map<Long, DimTables.Part>> parts = new HashMap<>();
    /** dbName -> (workcenterid -> 产线) */
    public Map<String, Map<Long, DimTables.WorkCenter>> workCenters = new HashMap<>();
    /** dbName -> (worksiteid -> 车间) */
    public Map<String, Map<Long, DimTables.WorkSite>> workSites = new HashMap<>();
    /** dbName -> (userid -> 用户) */
    public Map<String, Map<Long, DimTables.User>> users = new HashMap<>();

    /** 加载完成时间，用于判断缓存新鲜度和排查"维度全空是不是因为还没加载" */
    public long loadedAt;

    public DimCache() {
    }

    public DimTables.ProductionOrder order(String db, Long id) {
        Map<Long, DimTables.ProductionOrder> m = ordersById.get(db);
        return (m == null || id == null) ? null : m.get(id);
    }

    public DimTables.Part part(String db, Long id) {
        Map<Long, DimTables.Part> m = parts.get(db);
        return (m == null || id == null) ? null : m.get(id);
    }

    public DimTables.WorkCenter workCenter(String db, Long id) {
        Map<Long, DimTables.WorkCenter> m = workCenters.get(db);
        return (m == null || id == null) ? null : m.get(id);
    }

    public DimTables.WorkSite workSite(String db, Long id) {
        Map<Long, DimTables.WorkSite> m = workSites.get(db);
        return (m == null || id == null) ? null : m.get(id);
    }

    public DimTables.User user(String db, Long id) {
        Map<Long, DimTables.User> m = users.get(db);
        return (m == null || id == null) ? null : m.get(id);
    }

    /** 各维表总条数，启动日志里打出来，一眼看出是不是加载成功了 */
    public String summary() {
        return "DimCache{dbs=" + ordersById.size()
                + ", orders=" + count(ordersById)
                + ", parts=" + count(parts)
                + ", workCenters=" + count(workCenters)
                + ", workSites=" + count(workSites)
                + ", users=" + count(users)
                + ", loadedAt=" + loadedAt + '}';
    }

    private static int count(Map<String, ? extends Map<?, ?>> m) {
        int n = 0;
        for (Map<?, ?> v : m.values()) {
            n += v.size();
        }
        return n;
    }
}
