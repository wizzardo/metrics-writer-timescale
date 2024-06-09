package com.wizzardo.metrics.timescale.service;

import com.wizzardo.http.framework.di.PostConstruct;
import com.wizzardo.http.framework.di.Service;
import com.wizzardo.metrics.timescale.config.DataSourceConfig;
import com.wizzardo.tools.misc.TextTools;
import com.wizzardo.tools.misc.Unchecked;
import com.wizzardo.tools.sql.DBTools;
import com.wizzardo.tools.sql.SimpleConnectionPool;
import org.postgresql.ds.PGConnectionPoolDataSource;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;

public class DBService extends DBTools implements Service, PostConstruct {

    DataSourceConfig dataSourceConfig;

    public void init() {
        this.dataSource = new SimpleConnectionPool(createDatasource(), 4);
        migrate();
    }

    private ConnectionPoolDataSource createDatasource() {
        var datasourceConfig = dataSourceConfig;
        var username = datasourceConfig.username;
        var password = datasourceConfig.password;
        var url = datasourceConfig.url;

        var dbName = TextTools.substringAfterLast(url, "/");
        var host = TextTools.substringBefore(TextTools.substringAfter(url, "://"), "/");

        var poolDataSource = new PGConnectionPoolDataSource();
        poolDataSource.setDatabaseName(TextTools.substringBefore(dbName, "?"));
        poolDataSource.setServerNames(new String[]{TextTools.substringBefore(host, ":")});
        poolDataSource.setPortNumbers(new int[]{TextTools.asInt(TextTools.substringAfter(host, ":"), 5432)});
        poolDataSource.setUser(username);
        poolDataSource.setPassword(password);
        poolDataSource.setBinaryTransfer(true);
        poolDataSource.setTcpKeepAlive(true);
        poolDataSource.setPreparedStatementCacheSizeMiB(1);
        poolDataSource.setPreparedStatementCacheQueries(32);
//        poolDataSource.ssl = true;
//        poolDataSource.sslMode = "require";
        poolDataSource.setSslMode("disable");
        return poolDataSource;
    }

    public <R> R withDBTransaction(Unchecked.Consumer<Connection, R> mapper) {
        return withDB(c -> {
            c.setAutoCommit(false);
            try {
                R result = mapper.call(c);
                c.commit();
                return result;
            } catch (Exception e) {
                c.rollback();
                throw new RuntimeException(e);
            } finally {
                c.setAutoCommit(true);
            }
        });
    }

}