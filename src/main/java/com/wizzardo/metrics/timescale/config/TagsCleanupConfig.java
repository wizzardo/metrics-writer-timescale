package com.wizzardo.metrics.timescale.config;

import com.wizzardo.http.framework.Configuration;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TagsCleanupConfig implements Configuration {

    public String tags = "";
    public String startTime = "";
    public int batchSize = 1000;
    public long batchPauseMs = 50;
    public String schema = "metrics";
    public boolean enabled = true;

    @Override
    public String prefix() {
        return "tags_cleanup";
    }

    public TagsCleanupConfig() {
    }

    public TagsCleanupConfig(String tags, String startTime, int batchSize, String schema, boolean enabled) {
        this(tags, startTime, batchSize, 50, schema, enabled);
    }

    public TagsCleanupConfig(String tags, String startTime, int batchSize, long batchPauseMs, String schema, boolean enabled) {
        this.tags = tags;
        this.startTime = startTime;
        this.batchSize = batchSize;
        this.batchPauseMs = batchPauseMs;
        this.schema = schema;
        this.enabled = enabled;
    }

    public List<String> getTags() {
        String effectiveTags = (tags != null && !tags.trim().isEmpty()) ? tags : System.getenv("TAGS_CLEANUP_TAGS");
        if (effectiveTags == null || effectiveTags.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(effectiveTags.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    public List<String> getTagColumnNames() {
        return getTags().stream()
                .map(TagsCleanupConfig::toColumnName)
                .toList();
    }

    public static String toColumnName(String name) {
        if (name == null) {
            return null;
        }
        name = name.toLowerCase();
        if (name.equals("id")) {
            return "_id";
        }
        return name.replaceAll("\\W+", "_");
    }

    public String getStartTime() {
        if (startTime != null && !startTime.trim().isEmpty()) {
            return startTime.trim();
        }
        String env = System.getenv("TAGS_CLEANUP_START_TIME");
        return env != null ? env.trim() : "";
    }

    public LocalTime getParsedStartTime() {
        String st = getStartTime();
        if (st == null || st.isEmpty()) {
            return null;
        }
        return LocalTime.parse(st);
    }

    public int getBatchSize() {
        if (batchSize > 0 && batchSize != 1000) {
            return batchSize;
        }
        String env = System.getenv("TAGS_CLEANUP_BATCH_SIZE");
        if (env != null && !env.trim().isEmpty()) {
            try {
                return Integer.parseInt(env.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return batchSize > 0 ? batchSize : 1000;
    }

    public long getBatchPauseMs() {
        if (batchPauseMs >= 0 && batchPauseMs != 50) {
            return batchPauseMs;
        }
        String env = System.getenv("TAGS_CLEANUP_BATCH_PAUSE_MS");
        if (env == null || env.trim().isEmpty()) {
            env = System.getenv("TAGS_CLEANUP_PAUSE_BETWEEN_BATCHES_MS");
        }
        if (env != null && !env.trim().isEmpty()) {
            try {
                return Long.parseLong(env.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return batchPauseMs >= 0 ? batchPauseMs : 50;
    }

    public String getSchema() {
        if (schema != null && !schema.trim().isEmpty() && !schema.equals("metrics")) {
            return schema.trim();
        }
        String env = System.getenv("TAGS_CLEANUP_SCHEMA");
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return schema != null && !schema.trim().isEmpty() ? schema.trim() : "metrics";
    }

    public boolean isEnabled() {
        String env = System.getenv("TAGS_CLEANUP_ENABLED");
        if (env != null && !env.trim().isEmpty()) {
            return Boolean.parseBoolean(env.trim());
        }
        return enabled;
    }

    public static long calculateDelayUntilNextRun(LocalTime targetTime) {
        return calculateDelayUntilNextRun(targetTime, Clock.systemUTC());
    }

    public static long calculateDelayUntilNextRun(LocalTime targetTime, Clock clock) {
        if (targetTime == null) {
            throw new IllegalArgumentException("targetTime cannot be null");
        }
        ZonedDateTime nowUtc = ZonedDateTime.now(clock.withZone(ZoneOffset.UTC));
        ZonedDateTime nextRun = nowUtc.with(targetTime);
        if (!nextRun.isAfter(nowUtc)) {
            nextRun = nextRun.plusDays(1);
        }
        return Duration.between(nowUtc, nextRun).toMillis();
    }
}
