package top.meethigher.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import top.meethigher.test.utils.PGSizeQuery;

@Slf4j
public abstract class BloatedIndexRunner implements CommandLineRunner {

    private final Long totalNum;

    private final PGSizeQuery pgSizeQuery;

    private final Integer startTotal = 2000;

    private Long startIndexSize = 0L;

    private Long startDataSize = 0L;

    private Long endIndexSize = 0L;

    private Long endDataSize = 0L;

    private final String name;


    protected BloatedIndexRunner(Long totalNum, PGSizeQuery pgSizeQuery, String name) {
        this.totalNum = totalNum;
        this.pgSizeQuery = pgSizeQuery;
        this.name = name;
    }


    @Override
    public void run(String... args) throws Exception {
        log.info("{} start", name);
        recreateTable();
        runToEnd();
        log.info("{} end", name);
    }

    public void recreateTable() {
        String sql = "-- 重建表\n" +
                "drop table if exists test_data;\n" +
                "create table test_data (\n" +
                "    id serial,\n" +
                "    name varchar,\n" +
                "    age int4\n" +
                ");\n" +
                "\n" +
                "-- 关闭自动vacuum\n" +
                "alter table test_data set (autovacuum_enabled = false);\n" +
                "\n" +
                "\n" +
                "-- 插入模拟数据\n" +
                "insert into test_data (name, age)\n" +
                "select concat('Name_',generate_series(1, {startTotal})),  (random() * 100)::int4;\n" +
                "\n" +
                "\n" +
                "-- 创建索引\n" +
                "create index bloatedindex_test on test_data (age);\n" +
                "create index bloatedindex_test1 on test_data (name);";
        sql = sql.replace("{startTotal}", getStartTotal().toString());
        JdbcTemplate jdbcTemplate = getPgSizeQuery().getJdbcTemplate();
        jdbcTemplate.update(sql);
        PGSizeQuery.PGSize indexesSize = getPgSizeQuery().getIndexesSize("test_data");
        setStartIndexSize(indexesSize.getSize());
        PGSizeQuery.PGSize dataSize = getPgSizeQuery().getDataSize("test_data");
        setStartDataSize(dataSize.getSize());
        log.info("startIndexSize:{},startDataSize:{}", indexesSize.getPrettySize(), dataSize.getPrettySize());
    }

    public void runToEnd() {
        final long total = getTotalNum();
        for (long i = 1; i <= total; i++) {
            execute(i);
        }
        PGSizeQuery.PGSize indexesSize = getPgSizeQuery().getIndexesSize("test_data");
        setStartIndexSize(indexesSize.getSize());
        PGSizeQuery.PGSize dataSize = getPgSizeQuery().getDataSize("test_data");
        setEndDataSize(dataSize.getSize());
        log.info("endIndexSize:{},endDataSize:{}", indexesSize.getPrettySize(), dataSize.getPrettySize());
        log.info("deadTup:{}", getPgSizeQuery().getDeadTup("test_data"));

    }

    public abstract void execute(long index);


    public Long getTotalNum() {
        return totalNum;
    }

    public PGSizeQuery getPgSizeQuery() {
        return pgSizeQuery;
    }

    public Long getStartIndexSize() {
        return startIndexSize;
    }

    public Long getEndIndexSize() {
        return endIndexSize;
    }

    public void setStartIndexSize(Long startIndexSize) {
        this.startIndexSize = startIndexSize;
    }

    public void setEndIndexSize(Long endIndexSize) {
        this.endIndexSize = endIndexSize;
    }

    public Long getStartDataSize() {
        return startDataSize;
    }

    public Long getEndDataSize() {
        return endDataSize;
    }

    public void setStartDataSize(Long startDataSize) {
        this.startDataSize = startDataSize;
    }

    public void setEndDataSize(Long endDataSize) {
        this.endDataSize = endDataSize;
    }

    public Integer getStartTotal() {
        return startTotal;
    }

    public String getName() {
        return name;
    }
}
