package com.wizzardo.metrics.timescale.config;

import com.wizzardo.http.framework.Configuration;

public class DataSourceConfig implements Configuration {

    public final String url;
    public final String username;
    public final String password;

    @Override
    public String prefix() {
        return "datasource";
    }

    public DataSourceConfig() {
        this("", "", "");
    }

    protected DataSourceConfig(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }
}
