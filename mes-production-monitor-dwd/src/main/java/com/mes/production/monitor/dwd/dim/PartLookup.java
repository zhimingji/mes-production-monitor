package com.mes.production.monitor.dwd.dim;

import com.mes.production.monitor.common.model.DimTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * part 大维表的按需查询缓存（不进广播态）
 *
 * <p>part 是 20 万/基地 × 10 基地 = 200 万级主数据，整表广播会把 TaskManager 打 OOM
 * （Kryo 引用追踪序列化千万级对象）。但 DWD 只经 {@code order.partId} 访问 part，真正需要的
 * 是「被订单引用到的那些物料」，工作集远小于主数据，所以改成有界 LRU + JDBC 按需查：
 * 命中直接返回，miss 时查一次 MySQL 并缓存。
 *
 * <p>单条查询在 processElement 里是同步阻塞的——这是用简单换正确。part 只在手动通道、
 * 且 order 命中时才查，冷启动 miss 有限；真要高吞吐再换成 Flink Async I/O。
 *
 * <p>每个 subtask 各自维护一份缓存和连接（{@code transient}，不随函数对象序列化），
 * 由使用方在 {@code open()} 里创建、{@link #close} 里释放。
 */
public class PartLookup {

    private static final Logger LOG = LoggerFactory.getLogger(PartLookup.class);

    private final String jdbcUrlTemplate;
    private final String username;
    private final String password;
    private final int maxSize;

    private transient Map<PartKey, DimTables.Part> cache;
    private transient Map<String, Connection> connections;

    public PartLookup(String jdbcUrlTemplate, String username, String password, int maxSize) {
        this.jdbcUrlTemplate = jdbcUrlTemplate;
        this.username = username;
        this.password = password;
        this.maxSize = maxSize;
    }

    public void open() {
        // accessOrder=true 实现 LRU：每次 get 把命中条目移到队尾，队头即最久未用
        this.cache = new LinkedHashMap<PartKey, DimTables.Part>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PartKey, DimTables.Part> eldest) {
                return size() > maxSize;
            }
        };
        this.connections = new HashMap<>();
    }

    /** 按 (db, partId) 查 part。miss 返回 null（调用方计 partMissCount），不抛异常 */
    public DimTables.Part lookup(String db, Long partId) {
        if (db == null || partId == null) {
            return null;
        }
        PartKey key = new PartKey(db, partId);
        DimTables.Part p = cache.get(key);
        if (p != null) {
            return p;
        }
        try {
            p = query(db, partId);
        } catch (SQLException e) {
            // 单条查询失败不能让作业挂掉：维度留空 + 计数，与广播态维表 miss 同一套语义
            LOG.warn("part 查询失败 db={} partId={}，本条维度留空", db, partId, e);
            return null;
        }
        if (p != null) {
            cache.put(key, p);
        }
        return p;
    }

    private DimTables.Part query(String db, long partId) throws SQLException {
        Connection conn = connection(db);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT partid, partcode, partname FROM part WHERE partid = ?")) {
            ps.setLong(1, partId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    DimTables.Part p = new DimTables.Part();
                    p.partId = rs.getLong("partid");
                    p.partCode = rs.getString("partcode");
                    p.partName = rs.getString("partname");
                    return p;
                }
            }
        }
        return null;
    }

    /** 每基地一个连接（最多 10 个），懒建立，close 时统一释放 */
    private Connection connection(String db) throws SQLException {
        Connection c = connections.get(db);
        if (c == null || c.isClosed()) {
            c = DriverManager.getConnection(String.format(jdbcUrlTemplate, db), username, password);
            connections.put(db, c);
        }
        return c;
    }

    public void close() {
        for (Connection c : connections.values()) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    LOG.warn("关闭 part 查询连接失败", e);
                }
            }
        }
        connections.clear();
        cache.clear();
    }

    /** (db, partId) 复合键：partId 只在库内唯一（各基地独立） */
    private static final class PartKey {
        private final String db;
        private final long partId;

        PartKey(String db, long partId) {
            this.db = db;
            this.partId = partId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartKey)) {
                return false;
            }
            PartKey other = (PartKey) o;
            return partId == other.partId && db.equals(other.db);
        }

        @Override
        public int hashCode() {
            return 31 * db.hashCode() + Long.hashCode(partId);
        }
    }
}
