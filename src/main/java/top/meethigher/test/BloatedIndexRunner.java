package top.meethigher.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import top.meethigher.test.utils.PGSizeQuery;

@Slf4j
public abstract class BloatedIndexRunner implements CommandLineRunner {

    private final Long totalNum;

    private final PGSizeQuery pgSizeQuery;

    private final Long startTotal;

    private Long startIndexSize = 0L;

    private Long startDataSize = 0L;

    private Long startDeadTup = 0L;

    private Long endIndexSize = 0L;

    private Long endDataSize = 0L;

    private Long endDeadTup = 0L;

    private final String name;


    protected BloatedIndexRunner(Long startTotal, Long totalNum, PGSizeQuery pgSizeQuery, String name) {
        this.startTotal = startTotal;
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
        setStartDeadTup(getPgSizeQuery().getDeadTup("test_data"));
        log.info("startIndexSize:{},startDataSize:{},startDeadTup:{}", indexesSize.getPrettySize(), dataSize.getPrettySize(), getStartDeadTup());
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
        setEndDeadTup(getPgSizeQuery().getDeadTup("test_data"));
        log.info("endIndexSize:{},endDataSize:{},endDeadTup:{}", indexesSize.getPrettySize(), dataSize.getPrettySize(), getEndDeadTup());
    }

    public abstract void execute(long index);


    public Long getTotalNum() {
        return totalNum;
    }

    public PGSizeQuery getPgSizeQuery() {
        return pgSizeQuery;
    }

    public Long getStartTotal() {
        return startTotal;
    }

    public Long getStartIndexSize() {
        return startIndexSize;
    }

    public void setStartIndexSize(Long startIndexSize) {
        this.startIndexSize = startIndexSize;
    }

    public Long getStartDataSize() {
        return startDataSize;
    }

    public void setStartDataSize(Long startDataSize) {
        this.startDataSize = startDataSize;
    }

    public Long getStartDeadTup() {
        return startDeadTup;
    }

    public void setStartDeadTup(Long startDeadTup) {
        this.startDeadTup = startDeadTup;
    }

    public Long getEndIndexSize() {
        return endIndexSize;
    }

    public void setEndIndexSize(Long endIndexSize) {
        this.endIndexSize = endIndexSize;
    }

    public Long getEndDataSize() {
        return endDataSize;
    }

    public void setEndDataSize(Long endDataSize) {
        this.endDataSize = endDataSize;
    }

    public Long getEndDeadTup() {
        return endDeadTup;
    }

    public void setEndDeadTup(Long endDeadTup) {
        this.endDeadTup = endDeadTup;
    }

    public String getName() {
        return name;
    }
}
