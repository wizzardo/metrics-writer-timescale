package com.wizzardo.metrics.timescale.model;

import java.util.List;

public class MetricData {
    public String name;
    public double value;
    public List<List<String>> tags;
    public long timestamp;
}
