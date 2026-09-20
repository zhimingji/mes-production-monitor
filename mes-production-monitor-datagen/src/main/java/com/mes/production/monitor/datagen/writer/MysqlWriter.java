package com.mes.production.monitor.datagen.writer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MySQL 批量写入器，每个库一个连接池。
 *
 * <p>造数是整个链路的写入瓶颈（不是 Flink），所以这里必须用批量 + 连接池。
 * 逐条 insert 造 25 万条要跑很久，批量后是分钟级。
 */
public class MysqlWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlWriter.class);

    private final String urlTemplate;
    private final String username;
    private final String password;
    private final int batchSize;
    private final Map<String, HikariDataSource> pools = new HashMap<>();

    public MysqlWriter(String urlTemplate, String username, String password, int batchSize) {
        this.urlTemplate = urlTemplate;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
    }

    private synchronized HikariDataSource pool(String db) {
        return pools.computeIfAbsent(db, d -> {
            HikariConfig cfg = new HikariConfig();
            cfg.setJdbcUrl(String.format(urlTemplate, d));
            cfg.setUsername(username);
            cfg.setPassword(password);
            cfg.setMaximumPoolSize(4);
            cfg.setPoolName("datagen-" + d);
            // rewriteBatchedStatements 是 MySQL 批量写入提速的关键，
            // 不开的话 addBatch 仍然是逐条发送，性能差一个数量级
            cfg.addDataSourceProperty("rewriteBatchedStatements", "true");
            return new HikariDataSource(cfg);
        });
    }

    /**
     * 批量执行。
     *
     * @param db   目标库名，如 {@code mes_old_zh}
     * @param sql  带占位符的 SQL
     * @param rows 每行的参数数组，顺序与占位符一致
     */
    public void batch(String db, String sql, List<Object[]> rows) {
        if (rows.isEmpty()) {
            return;
        }
        try (Connection conn = pool(db).getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int n = 0;
                for (Object[] row : rows) {
                    for (int i = 0; i < row.length; i++) {
                        ps.setObject(i + 1, row[i]);
                    }
                    ps.addBatch();
                    if (++n % batchSize == 0) {
                        ps.executeBatch();
                        conn.commit();
                    }
                }
                ps.executeBatch();
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("批量写入失败, db=" + db + ", sql=" + sql, e);
        }
    }

    /** 单条执行，用于报工修正的 UPDATE 等零散操作 */
    public int execute(String db, String sql, Object... params) {
        try (Connection conn = pool(db).getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("执行失败, db=" + db + ", sql=" + sql, e);
        }
    }

    @Override
    public void close() {
        pools.values().forEach(HikariDataSource::close);
        LOG.info("关闭 {} 个连接池", pools.size());
    }
}
