package com.wizzardo.metrics.timescale;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.config.DataSourceConfig;
import com.wizzardo.metrics.timescale.service.DBService;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class IntegrationTestBase {
    public IntegrationTestBase() {
        var postgreSQLContainer = new PostgreSQLContainer<>(DockerImageName.parse("timescale/timescaledb:latest-pg18").asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("integration-tests-db")
                .withUsername("sa")
                .withPassword("sa");

        postgreSQLContainer.start();

        DataSourceConfig config = new DataSourceConfig(postgreSQLContainer.getJdbcUrl(), postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword());

        var dbService = new DBService(config);
        dbService.init();
        DependencyFactory.get().register(DBService.class, dbService);

    }
}
