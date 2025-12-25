package com.wizzardo.metrics.timescale.db.model;

public class Hypertables {
    String hypertableSchema;
    String hypertableName;
    String owner;
    short numDimensions;
    long numChunks;
    boolean compressionEnabled;
    String[] tablespaces;
    String primaryDimension;
    String primaryDimensionType;
}
