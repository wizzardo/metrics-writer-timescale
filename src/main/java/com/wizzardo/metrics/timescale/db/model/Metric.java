package com.wizzardo.metrics.timescale.db.model;

import java.sql.Timestamp;

public class Metric {
    public Timestamp createdAt;
    public long tagsId;
    public double value;
}
