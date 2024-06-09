package com.wizzardo.metrics.timescale;

import com.wizzardo.http.framework.WebApplication;
import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.http.framework.template.Tag;
import com.wizzardo.metrics.timescale.handler.IngestHandler;
import com.wizzardo.metrics.timescale.service.DBService;

import java.util.Collections;
import java.util.List;

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

    @Override
    protected List<Class<? extends Tag>> getBasicTags() {
        return Collections.emptyList();
    }

    public static void main(String[] args) {
        WebApplication webApplication = new App(args);
        webApplication.start();
    }
}
