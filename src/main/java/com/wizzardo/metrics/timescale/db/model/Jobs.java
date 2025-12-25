package com.wizzardo.metrics.timescale.db.model;

import java.util.Date;

public class Jobs {
    int jobId;
    String applicationName;
    String scheduleInterval;
    String maxRuntime;
    int maxRetries;
    String retryPeriod;
    String procSchema;
    String procName;
    String owner;
    boolean scheduled;
    boolean fixedSchedule;
    String config;
    Date nextStart;
    Date initialStart;
    String hypertableSchema;
    String hypertableName;
    String checkSchema;
    String checkName;
}
