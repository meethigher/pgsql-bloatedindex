package top.meethigher.test.runner.insert;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import top.meethigher.test.BloatedIndexRunner;
import top.meethigher.test.utils.PGSizeQuery;

@Slf4j
public class InsertDeleteRunner extends BloatedIndexRunner {


    public InsertDeleteRunner(Long totalNum, PGSizeQuery pgSizeQuery, String name) {
        super(totalNum, pgSizeQuery, name);
    }

    @Override
    public void execute(long index) {
        JdbcTemplate jdbcTemplate = getPgSizeQuery().getJdbcTemplate();
        jdbcTemplate.update("with deleted_rows as (\n" +
                "  delete from test_data\n" +
                "  where id in (select id from test_data limit 1) \n" +
                "  returning name,age\n" +
                ")\n" +
                "insert into test_data (name, age)\n" +
                "select name,age from deleted_rows");
    }


}
