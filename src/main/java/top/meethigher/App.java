package top.meethigher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import top.meethigher.test.BloatedIndexRunner;
import top.meethigher.test.runner.insert.InsertDeleteRunner;
import top.meethigher.test.runner.update.UpdateRunner;
import top.meethigher.test.utils.PGSizeQuery;

import javax.sql.DataSource;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


    private final Long num = 10000L;

    @Bean
    public BloatedIndexRunner bloatedIndexRunner(PGSizeQuery pgSizeQuery) {
        return new InsertDeleteRunner(num, pgSizeQuery, "insertDelete");
    }


    @Bean
    public BloatedIndexRunner bloatedIndexRunner1(PGSizeQuery pgSizeQuery) {
        return new UpdateRunner(num, pgSizeQuery, "update");
    }

    @Bean
    public PGSizeQuery pgSizeQuery(JdbcTemplate jdbcTemplate) {
        return new PGSizeQuery(jdbcTemplate);
    }
}
