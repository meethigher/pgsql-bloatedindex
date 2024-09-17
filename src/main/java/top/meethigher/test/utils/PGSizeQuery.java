package top.meethigher.test.utils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class PGSizeQuery {

    private final JdbcTemplate jdbcTemplate;

    public PGSizeQuery(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    public PGSize getDataSize(String tableName) {
        String sql = "select pg_table_size('{tableName}') as size,pg_size_pretty(pg_table_size('{tableName}')) as prettySize";
        try {
            return jdbcTemplate.query(sql.replace("{tableName}", tableName), new BeanPropertyRowMapper<>(PGSize.class)).get(0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public PGSize getIndexesSize(String tableName) {
        String sql = "select pg_indexes_size('{tableName}') as size,pg_size_pretty(pg_indexes_size('{tableName}')) as prettySize";
        try {
            return jdbcTemplate.query(sql.replace("{tableName}", tableName), new BeanPropertyRowMapper<>(PGSize.class)).get(0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public PGSize getDataAndIndexesSize(String tableName) {
        String sql = "select pg_total_relation_size('{tableName}') as size,pg_size_pretty(pg_total_relation_size('{tableName}')) as prettySize";
        try {
            return jdbcTemplate.query(sql.replace("{tableName}", tableName), new BeanPropertyRowMapper<>(PGSize.class)).get(0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Long getDeadTup(String tableName) {
        try {
            return jdbcTemplate.queryForObject(" select n_dead_tup from pg_stat_user_tables where n_dead_tup > 0 and relname= ?", Long.class, tableName);
        } catch (Exception e) {
            return 0L;
        }

    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    @Data
    public static class PGSize {
        private Long size;
        private String prettySize;
    }

}


