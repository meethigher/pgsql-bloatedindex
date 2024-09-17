package top.meethigher.test.runner.update;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import top.meethigher.test.BloatedIndexRunner;
import top.meethigher.test.utils.PGSizeQuery;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class UpdateRunner extends BloatedIndexRunner {


    public UpdateRunner(Long totalNum, PGSizeQuery pgSizeQuery, String name) {
        super(totalNum, pgSizeQuery, name);
    }

    @Override
    public void execute(long index) {
        JdbcTemplate jdbcTemplate = getPgSizeQuery().getJdbcTemplate();
        long id = index % getStartTotal();
        jdbcTemplate.update("update test_data set name=?,age=? where id = ?", "Name_" + index, ThreadLocalRandom.current().nextInt(1, 100), id);
    }


}
