package com.wizzardo.metrics.timescale.service;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.IntegrationTestBase;
import com.wizzardo.metrics.timescale.config.TagsCleanupConfig;
import com.wizzardo.metrics.timescale.handler.IngestHandler;
import com.wizzardo.metrics.timescale.model.MetricData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class TagsCleanupServiceTest extends IntegrationTestBase {

    private IngestHandler createHandler(DBService dbService) {
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;
        return handler;
    }

    private static MetricData metric(String name, double value, long timestampNano, List<List<String>> tags) {
        MetricData data = new MetricData();
        data.name = name;
        data.value = value;
        data.timestamp = timestampNano;
        data.tags = new ArrayList<>();
        if (tags != null) {
            for (List<String> tag : tags) {
                data.tags.add(new ArrayList<>(tag));
            }
        }
        return data;
    }

    @Test
    public void testCalculateDelayUntilNextRun() {
        Clock fixedClock = Clock.fixed(Instant.parse("2026-09-01T02:00:00Z"), ZoneOffset.UTC);

        // Run later today at 03:00 -> 1 hour delay
        long delay1 = TagsCleanupConfig.calculateDelayUntilNextRun(LocalTime.of(3, 0), fixedClock);
        Assertions.assertEquals(3600_000L, delay1);

        // Run earlier today at 01:00 -> 23 hours delay (tomorrow)
        long delay2 = TagsCleanupConfig.calculateDelayUntilNextRun(LocalTime.of(1, 0), fixedClock);
        Assertions.assertEquals(23 * 3600_000L, delay2);

        // Run exact same time -> 24 hours delay (tomorrow)
        long delay3 = TagsCleanupConfig.calculateDelayUntilNextRun(LocalTime.of(2, 0), fixedClock);
        Assertions.assertEquals(24 * 3600_000L, delay3);

        // Null target time throws exception
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                TagsCleanupConfig.calculateDelayUntilNextRun(null, fixedClock)
        );
    }

    @Test
    public void testConfigParsing() {
        TagsCleanupConfig config = new TagsCleanupConfig("host, pod , instance, id , app.version", "03:30:00", 500, 25, "metrics", true);
        Assertions.assertEquals(List.of("host", "pod", "instance", "id", "app.version"), config.getTags());
        Assertions.assertEquals(List.of("host", "pod", "instance", "_id", "app_version"), config.getTagColumnNames());
        Assertions.assertEquals("03:30:00", config.getStartTime());
        Assertions.assertEquals(LocalTime.of(3, 30, 0), config.getParsedStartTime());
        Assertions.assertEquals(500, config.getBatchSize());
        Assertions.assertEquals(25, config.getBatchPauseMs());
        Assertions.assertEquals("metrics", config.getSchema());
        Assertions.assertTrue(config.isEnabled());
        Assertions.assertEquals("tags_cleanup", config.prefix());

        TagsCleanupConfig defaultConfig = new TagsCleanupConfig();
        Assertions.assertEquals(50, defaultConfig.getBatchPauseMs());

        // Test toColumnName conversions
        Assertions.assertEquals("host", TagsCleanupConfig.toColumnName("host"));
        Assertions.assertEquals("_id", TagsCleanupConfig.toColumnName("id"));
        Assertions.assertEquals("my_special_tag", TagsCleanupConfig.toColumnName("my.special-tag"));
        Assertions.assertNull(TagsCleanupConfig.toColumnName(null));
    }

    @Test
    public void testCleanupOrphanedTags() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricName = "cleanup_test_m1_" + System.nanoTime();
        String ephemeralHost = "ephemeral-host-" + System.nanoTime();
        String activeHost = "active-host-" + System.nanoTime();

        MetricData m1 = metric(metricName, 10.0, 1_700_000_000_000_000_000L, List.of(List.of("host", ephemeralHost), List.of("env", "prod")));
        MetricData m2 = metric(metricName, 20.0, 1_700_000_000_000_000_000L, List.of(List.of("host", activeHost), List.of("env", "prod")));

        handler.handleMetrics(List.of(m1, m2));

        int ephemeralTagId = dbService.withBuilder(db -> handler.getTagId(db, ephemeralHost));
        int activeTagId = dbService.withBuilder(db -> handler.getTagId(db, activeHost));

        // Delete metric rows corresponding to ephemeralHost
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement(
                    "DELETE FROM metrics." + metricName + " WHERE tags_id IN (" +
                            "SELECT id FROM metrics._tags_" + metricName + " WHERE \"host\" = ?)"
            )) {
                st.setInt(1, ephemeralTagId);
                int deleted = st.executeUpdate();
                Assertions.assertTrue(deleted > 0);
            }
            return null;
        });

        // Run cleanup service
        TagsCleanupConfig config = new TagsCleanupConfig("host", "03:00", 1000, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);
        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();

        Assertions.assertTrue(summary.tablesScanned >= 1);
        Assertions.assertTrue(summary.tagValuesChecked >= 2);
        Assertions.assertTrue(summary.rowsDeleted >= 1);

        // Verify ephemeralHost is removed from _tags_<metricName>
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement(
                    "SELECT count(*) FROM metrics._tags_" + metricName + " WHERE \"host\" = ?"
            )) {
                st.setInt(1, ephemeralTagId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(0, rs.getInt(1), "Orphaned tag combination should have been deleted");
                }
            }

            // Verify activeHost is still present in _tags_<metricName>
            try (PreparedStatement st = c.prepareStatement(
                    "SELECT count(*) FROM metrics._tags_" + metricName + " WHERE \"host\" = ?"
            )) {
                st.setInt(1, activeTagId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getInt(1), "Active tag combination should still exist");
                }
            }
            return null;
        });
    }

    @Test
    public void testActiveTagsAreNotDeleted() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricName = "cleanup_active_m_" + System.nanoTime();
        String host1 = "active-h1-" + System.nanoTime();
        String host2 = "active-h2-" + System.nanoTime();

        MetricData m1 = metric(metricName, 1.0, 1_700_000_000_000_000_000L, List.of(List.of("host", host1), List.of("env", "staging")));
        MetricData m2 = metric(metricName, 2.0, 1_700_000_000_000_000_000L, List.of(List.of("host", host2), List.of("env", "staging")));

        handler.handleMetrics(List.of(m1, m2));

        TagsCleanupConfig config = new TagsCleanupConfig("host", "03:00", 1000, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);

        // Record tags count before
        int countBefore = dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName)) {
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    return rs.getInt(1);
                }
            }
        });
        Assertions.assertEquals(2, countBefore);

        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();
        Assertions.assertEquals(0, summary.rowsDeleted, "No rows should be deleted for active tags");

        // Verify count remains 2
        int countAfter = dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName)) {
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    return rs.getInt(1);
                }
            }
        });
        Assertions.assertEquals(2, countAfter);
    }

    @Test
    public void testBatchDeletion() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricName = "cleanup_batch_m_" + System.nanoTime();
        String hostName = "batch-orphan-host-" + System.nanoTime();

        // Initialize metric table by ingesting one initial point
        MetricData initMetric = metric(metricName, 1.0, 1_700_000_000_000_000_000L, List.of(List.of("host", hostName), List.of("pod", "pod-0")));
        handler.handleMetrics(List.of(initMetric));

        int hostTagId = dbService.withBuilder(db -> handler.getTagId(db, hostName));

        // Delete all metric entries so this host is completely orphaned
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("DELETE FROM metrics." + metricName)) {
                st.executeUpdate();
            }
            return null;
        });

        // Insert 2500 tag combinations directly into metrics._tags_<metricName>
        int totalOrphanRows = 2500;
        dbService.withDB(c -> {
            c.setAutoCommit(false);
            try (PreparedStatement st = c.prepareStatement(
                    "INSERT INTO metrics._tags_" + metricName + " (\"host\", \"pod\") VALUES (?, ?)"
            )) {
                for (int i = 1; i <= totalOrphanRows; i++) {
                    st.setInt(1, hostTagId);
                    st.setInt(2, i);
                    st.addBatch();
                    if (i % 500 == 0) {
                        st.executeBatch();
                    }
                }
                st.executeBatch();
                c.commit();
            } finally {
                c.setAutoCommit(true);
            }
            return null;
        });

        // Verify count in _tags table is 2501 (2500 inserted + 1 from initMetric)
        int initialTagsCount = dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName + " WHERE \"host\" = ?")) {
                st.setInt(1, hostTagId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    return rs.getInt(1);
                }
            }
        });
        Assertions.assertEquals(2501, initialTagsCount);

        // Run cleanup with batchSize = 1000
        TagsCleanupConfig config = new TagsCleanupConfig("host", "03:00", 1000, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);
        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();

        Assertions.assertTrue(summary.rowsDeleted >= 2501);

        // Verify remaining count in _tags table is 0
        int finalTagsCount = dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName + " WHERE \"host\" = ?")) {
                st.setInt(1, hostTagId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    return rs.getInt(1);
                }
            }
        });
        Assertions.assertEquals(0, finalTagsCount);
    }

    @Test
    public void testMissingTagColumnsHandled() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricName = "cleanup_missing_col_m_" + System.nanoTime();
        MetricData m = metric(metricName, 1.0, 1_700_000_000_000_000_000L, List.of(List.of("device", "sda1"), List.of("mount", "/var")));
        handler.handleMetrics(List.of(m));

        // Configure cleanup with tags that do NOT exist in disk_io ("non_existing_tag_xyz", "host")
        TagsCleanupConfig config = new TagsCleanupConfig("non_existing_tag_xyz, nonexistent2", "03:00", 1000, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);

        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();
        Assertions.assertTrue(summary.tablesScanned >= 1);
        Assertions.assertEquals(0, summary.tagValuesChecked);
        Assertions.assertEquals(0, summary.rowsDeleted);
    }

    @Test
    public void testMultipleConfiguredTagsAndMultipleTables() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricA = "cleanup_multi_a_" + System.nanoTime();
        String metricB = "cleanup_multi_b_" + System.nanoTime();

        String orphanHostA = "orphan-host-a-" + System.nanoTime();
        String activeHostA = "active-host-a-" + System.nanoTime();
        String orphanPodB = "orphan-pod-b-" + System.nanoTime();
        String activePodB = "active-pod-b-" + System.nanoTime();

        MetricData a1 = metric(metricA, 1.0, 1_700_000_000_000_000_000L, List.of(List.of("host", orphanHostA), List.of("region", "us-east")));
        MetricData a2 = metric(metricA, 2.0, 1_700_000_000_000_000_000L, List.of(List.of("host", activeHostA), List.of("region", "us-west")));

        MetricData b1 = metric(metricB, 10.0, 1_700_000_000_000_000_000L, List.of(List.of("pod", orphanPodB), List.of("service", "auth")));
        MetricData b2 = metric(metricB, 20.0, 1_700_000_000_000_000_000L, List.of(List.of("pod", activePodB), List.of("service", "web")));

        handler.handleMetrics(List.of(a1, a2, b1, b2));

        int orphanHostAId = dbService.withBuilder(db -> handler.getTagId(db, orphanHostA));
        int orphanPodBId = dbService.withBuilder(db -> handler.getTagId(db, orphanPodB));

        // Delete metric rows for orphanHostA in metricA and orphanPodB in metricB
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement(
                    "DELETE FROM metrics." + metricA + " WHERE tags_id IN (" +
                            "SELECT id FROM metrics._tags_" + metricA + " WHERE \"host\" = ?)"
            )) {
                st.setInt(1, orphanHostAId);
                st.executeUpdate();
            }
            try (PreparedStatement st = c.prepareStatement(
                    "DELETE FROM metrics." + metricB + " WHERE tags_id IN (" +
                            "SELECT id FROM metrics._tags_" + metricB + " WHERE \"pod\" = ?)"
            )) {
                st.setInt(1, orphanPodBId);
                st.executeUpdate();
            }
            return null;
        });

        TagsCleanupConfig config = new TagsCleanupConfig("host, pod", "03:00", 1000, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);
        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();

        Assertions.assertTrue(summary.tablesScanned >= 2);
        Assertions.assertTrue(summary.rowsDeleted >= 2);

        // Verify orphanHostA is deleted from metricA's tags table
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricA + " WHERE \"host\" = ?")) {
                st.setInt(1, orphanHostAId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(0, rs.getInt(1));
                }
            }
            // Verify orphanPodB is deleted from metricB's tags table
            try (PreparedStatement st = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricB + " WHERE \"pod\" = ?")) {
                st.setInt(1, orphanPodBId);
                try (ResultSet rs = st.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(0, rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testDisabledAndUnconfigured() {
        DBService dbService = DependencyFactory.get(DBService.class);

        // Empty tags
        TagsCleanupConfig emptyConfig = new TagsCleanupConfig("", "03:00", 1000, "metrics", true);
        TagsCleanupService emptyService = new TagsCleanupService(dbService, emptyConfig);
        emptyService.init();
        TagsCleanupService.TagsCleanupSummary summary = emptyService.runCleanup();
        Assertions.assertEquals(0, summary.tablesScanned);
        Assertions.assertEquals(0, summary.rowsDeleted);
        emptyService.stop();

        // Disabled
        TagsCleanupConfig disabledConfig = new TagsCleanupConfig("host", "03:00", 1000, "metrics", false);
        TagsCleanupService disabledService = new TagsCleanupService(dbService, disabledConfig);
        disabledService.init();
        disabledService.stop();
    }

    @Test
    public void testBatchDeletionWithPause() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = createHandler(dbService);

        String metricName = "cleanup_pause_m_" + System.nanoTime();
        String hostName = "pause-orphan-host-" + System.nanoTime();

        // Initialize metric table by ingesting one initial point
        MetricData initMetric = metric(metricName, 1.0, 1_700_000_000_000_000_000L, List.of(List.of("host", hostName), List.of("pod", "pod-0")));
        handler.handleMetrics(List.of(initMetric));

        int hostTagId = dbService.withBuilder(db -> handler.getTagId(db, hostName));

        // Delete all metric entries so this host is completely orphaned
        dbService.withDB(c -> {
            try (PreparedStatement st = c.prepareStatement("DELETE FROM metrics." + metricName)) {
                st.executeUpdate();
            }
            return null;
        });

        // Insert 200 tag combinations directly into metrics._tags_<metricName>
        int totalOrphanRows = 200;
        dbService.withDB(c -> {
            c.setAutoCommit(false);
            try (PreparedStatement st = c.prepareStatement(
                    "INSERT INTO metrics._tags_" + metricName + " (\"host\", \"pod\") VALUES (?, ?)"
            )) {
                for (int i = 1; i <= totalOrphanRows; i++) {
                    st.setInt(1, hostTagId);
                    st.setInt(2, i);
                    st.addBatch();
                }
                st.executeBatch();
                c.commit();
            } finally {
                c.setAutoCommit(true);
            }
            return null;
        });

        // Run cleanup with batchSize = 100, batchPauseMs = 150
        TagsCleanupConfig config = new TagsCleanupConfig("host", "03:00", 100, 150, "metrics", true);
        TagsCleanupService cleanupService = new TagsCleanupService(dbService, config);
        long startTime = System.currentTimeMillis();
        TagsCleanupService.TagsCleanupSummary summary = cleanupService.runCleanup();
        long duration = System.currentTimeMillis() - startTime;

        Assertions.assertTrue(summary.rowsDeleted >= 201);
        Assertions.assertTrue(duration >= 250, "Duration should reflect batch pauses, was: " + duration + " ms");
    }
}
