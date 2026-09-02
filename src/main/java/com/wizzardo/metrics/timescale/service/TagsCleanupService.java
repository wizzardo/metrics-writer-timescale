package com.wizzardo.metrics.timescale.service;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.http.framework.di.Injectable;
import com.wizzardo.http.framework.di.PostConstruct;
import com.wizzardo.http.framework.di.Service;
import com.wizzardo.metrics.timescale.config.TagsCleanupConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Injectable
public class TagsCleanupService implements Service, PostConstruct {

    DBService dbService;
    TagsCleanupConfig config;

    protected Thread schedulerThread;
    protected volatile boolean running = true;

    public static class TagsCleanupSummary {
        public int tablesScanned;
        public int tagValuesChecked;
        public int rowsDeleted;
        public long durationMs;

        @Override
        public String toString() {
            return "TagsCleanupSummary{" +
                    "tablesScanned=" + tablesScanned +
                    ", tagValuesChecked=" + tagValuesChecked +
                    ", rowsDeleted=" + rowsDeleted +
                    ", durationMs=" + durationMs +
                    '}';
        }
    }

    public TagsCleanupService() {
    }

    public TagsCleanupService(DBService dbService, TagsCleanupConfig config) {
        this.dbService = dbService;
        this.config = config;
    }

    @Override
    public void init() {
        if (config == null) {
            try {
                config = DependencyFactory.get(TagsCleanupConfig.class);
            } catch (Exception ignored) {
                config = new TagsCleanupConfig();
            }
        }
        if (dbService == null) {
            try {
                dbService = DependencyFactory.get(DBService.class);
            } catch (Exception ignored) {
            }
        }
        if (!config.isEnabled()) {
            System.out.println("TagsCleanupService: service is disabled");
            return;
        }

        List<String> tags = config.getTags();
        String startTime = config.getStartTime();
        if (tags.isEmpty()) {
            System.out.println("TagsCleanupService: no tags configured (TAGS_CLEANUP_TAGS is empty), skipping scheduled runner");
            return;
        }
        if (startTime.isEmpty()) {
            System.out.println("TagsCleanupService: no startTime configured (TAGS_CLEANUP_START_TIME is empty), skipping scheduled runner");
            return;
        }

        LocalTime parsedStartTime;
        try {
            parsedStartTime = config.getParsedStartTime();
        } catch (Exception e) {
            System.err.println("TagsCleanupService: failed to parse startTime '" + startTime + "': " + e.getMessage());
            return;
        }

        startScheduler(parsedStartTime);
    }

    protected void startScheduler(LocalTime startTime) {
        schedulerThread = Thread.ofVirtual().name("tags-cleanup-thread").start(() -> {
            System.out.println("TagsCleanupService: scheduler started for daily run at " + startTime + " UTC");
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    long delay = TagsCleanupConfig.calculateDelayUntilNextRun(startTime);
                    System.out.println("TagsCleanupService: next run in " + (delay / 1000) + " seconds");
                    Thread.sleep(delay);
                    if (!running || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    runCleanup();
                } catch (InterruptedException e) {
                    System.out.println("TagsCleanupService: scheduler interrupted, stopping");
                    break;
                } catch (Throwable t) {
                    System.err.println("TagsCleanupService: error during scheduled cleanup execution: " + t.getMessage());
                    t.printStackTrace();
                    try {
                        Thread.sleep(60_000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
    }

    public void stop() {
        running = false;
        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }
    }

    public TagsCleanupSummary runCleanup() {
        TagsCleanupSummary summary = new TagsCleanupSummary();
        long start = System.currentTimeMillis();

        if (config == null) {
            config = new TagsCleanupConfig();
        }
        if (dbService == null) {
            dbService = DependencyFactory.get(DBService.class);
        }

        List<String> tags = config.getTags();
        if (tags.isEmpty()) {
            System.out.println("TagsCleanupService.runCleanup: no tags configured, nothing to clean");
            summary.durationMs = System.currentTimeMillis() - start;
            return summary;
        }

        String schema = config.getSchema();
        int batchSize = config.getBatchSize();
        long batchPauseMs = config.getBatchPauseMs();

        System.out.println("TagsCleanupService.runCleanup started for schema: " + schema + ", tags: " + tags + ", batchSize: " + batchSize);

        try {
            dbService.withDB(c -> {
                List<String> tagsTables = getMetricTagsTables(c, schema);
                for (String tagsTable : tagsTables) {
                    summary.tablesScanned++;
                    String metricTable = tagsTable.substring("_tags_".length());
                    System.out.println("TagsCleanupService: cleaning table " + tagsTable);
                    long tableStart = System.currentTimeMillis();
                    int rowsDeletedBefore = summary.rowsDeleted;

                    Set<String> columns = getTableColumns(c, schema, tagsTable);
                    for (String tag : tags) {
                        String tagColumn = TagsCleanupConfig.toColumnName(tag);
                        if (columns.contains(tagColumn)) {
                            cleanupTableTag(c, schema, tagsTable, metricTable, tagColumn, batchSize, batchPauseMs, summary);
                        }
                    }

                    long tableDuration = System.currentTimeMillis() - tableStart;
                    int tableRowsDeleted = summary.rowsDeleted - rowsDeletedBefore;
                    System.out.println("TagsCleanupService: finished cleaning table " + tagsTable + " in " + tableDuration + " ms (deleted " + tableRowsDeleted + " rows)");
                }
                return null;
            });
        } catch (Exception e) {
            System.err.println("TagsCleanupService.runCleanup failed: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            summary.durationMs = System.currentTimeMillis() - start;
            System.out.println("TagsCleanupService.runCleanup finished: " + summary);
        }

        return summary;
    }

    public List<String> getMetricTagsTables(Connection c, String schema) throws SQLException {
        List<String> tagsTables = new ArrayList<>();
        Set<String> allTables = new HashSet<>();
        try (PreparedStatement st = c.prepareStatement("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ?")) {
            st.setString(1, schema);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    allTables.add(rs.getString(1));
                }
            }
        }
        for (String table : allTables) {
            if (table.startsWith("_tags_")) {
                String metricTable = table.substring("_tags_".length());
                if (allTables.contains(metricTable)) {
                    tagsTables.add(table);
                }
            }
        }
        return tagsTables;
    }

    public Set<String> getTableColumns(Connection c, String schema, String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (PreparedStatement st = c.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")) {
            st.setString(1, schema);
            st.setString(2, tableName);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            }
        }
        return columns;
    }

    public List<Integer> getDistinctTagIds(Connection c, String schema, String tagsTableName, String tagColumn) throws SQLException {
        List<Integer> tagIds = new ArrayList<>();
        String sql = "SELECT DISTINCT \"" + tagColumn + "\" FROM \"" + schema + "\".\"" + tagsTableName + "\" WHERE \"" + tagColumn + "\" IS NOT NULL";
        try (PreparedStatement st = c.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
                tagIds.add(rs.getInt(1));
            }
        }
        return tagIds;
    }

    public boolean isTagReferenced(Connection c, String schema, String tagsTableName, String metricTableName, String tagColumn, int tagId) throws SQLException {
        String sql = "SELECT 1 FROM \"" + schema + "\".\"" + metricTableName + "\" WHERE tags_id IN (" +
                "SELECT id FROM \"" + schema + "\".\"" + tagsTableName + "\" WHERE \"" + tagColumn + "\" = ?" +
                ") LIMIT 1";
        try (PreparedStatement st = c.prepareStatement(sql)) {
            st.setInt(1, tagId);
            try (ResultSet rs = st.executeQuery()) {
                return rs.next();
            }
        }
    }

    public int deleteBatch(Connection c, String schema, String tagsTableName, String tagColumn, int tagId, int batchSize) throws SQLException {
        long batchStart = System.currentTimeMillis();
        String sql = "DELETE FROM \"" + schema + "\".\"" + tagsTableName + "\" WHERE id IN (" +
                "SELECT id FROM \"" + schema + "\".\"" + tagsTableName + "\" WHERE \"" + tagColumn + "\" = ? LIMIT ?" +
                ")";
        int deleted;
        try (PreparedStatement st = c.prepareStatement(sql)) {
            st.setInt(1, tagId);
            st.setInt(2, batchSize);
            deleted = st.executeUpdate();
        }
        long batchDuration = System.currentTimeMillis() - batchStart;
        System.out.println("TagsCleanupService: deleted batch of " + deleted + " rows from " + tagsTableName + " (" + tagColumn + "=" + tagId + ") in " + batchDuration + " ms");
        return deleted;
    }

    public void cleanupTableTag(Connection c, String schema, String tagsTableName, String metricTableName, String tagColumn, int batchSize, TagsCleanupSummary summary) throws SQLException {
        cleanupTableTag(c, schema, tagsTableName, metricTableName, tagColumn, batchSize, config != null ? config.getBatchPauseMs() : 50, summary);
    }

    public void cleanupTableTag(Connection c, String schema, String tagsTableName, String metricTableName, String tagColumn, int batchSize, long batchPauseMs, TagsCleanupSummary summary) throws SQLException {
        List<Integer> tagIds = getDistinctTagIds(c, schema, tagsTableName, tagColumn);
        for (int tagId : tagIds) {
            summary.tagValuesChecked++;
            boolean referenced = isTagReferenced(c, schema, tagsTableName, metricTableName, tagColumn, tagId);
            if (!referenced) {
                int deleted;
                do {
                    deleted = deleteBatch(c, schema, tagsTableName, tagColumn, tagId, batchSize);
                    summary.rowsDeleted += deleted;
                    if (deleted == batchSize && batchPauseMs > 0) {
                        try {
                            Thread.sleep(batchPauseMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } while (deleted == batchSize);
            }
        }
    }
}
