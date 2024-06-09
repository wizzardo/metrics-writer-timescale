package com.wizzardo.metrics.timescale;

import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.handler.IngestHandler;
import com.wizzardo.metrics.timescale.service.DBService;

public class App extends WebApplication {
    public App(String[] args) {
        super(args);

        onSetup(app -> {
            app.setDebugOutput(false);
            DependencyFactory.get(DBService.class); // force migrations
            app.getUrlMapping()
                    .append("/v1/metrics", IngestHandler.class)
            ;
        });
    }

    public static void main(String[] args) {
        WebApplication webApplication = new App(args);
        webApplication.start();
    }
}
