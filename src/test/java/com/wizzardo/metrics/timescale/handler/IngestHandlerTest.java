package com.wizzardo.metrics.timescale.handler;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.IntegrationTestBase;
import com.wizzardo.metrics.timescale.model.MetricData;
import com.wizzardo.metrics.timescale.service.DBService;
import com.wizzardo.tools.misc.Pair;
import com.wizzardo.tools.sql.query.Field;
import com.wizzardo.tools.sql.query.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IngestHandlerTest extends IntegrationTestBase {

    @Test
    public void testGetTagId() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();

        // 1. null returns null
        Integer nullTagId = dbService.withBuilder(db -> handler.getTagId(db, null));
        Assertions.assertNull(nullTagId);

        // 2. insert new tag
        String tagName1 = "env_prod_" + System.nanoTime();
        Integer tagId1 = dbService.withBuilder(db -> handler.getTagId(db, tagName1));
        Assertions.assertNotNull(tagId1);
        Assertions.assertTrue(tagId1 > 0);

        // 3. same tag should return same id (cached)
        Integer tagId1Cached = dbService.withBuilder(db -> handler.getTagId(db, tagName1));
        Assertions.assertEquals(tagId1, tagId1Cached);

        // 4. same tag on a new handler instance (bypassing cache) should return same id from DB
        IngestHandler handler2 = new IngestHandler();
        Integer tagId1FromDb = dbService.withBuilder(db -> handler2.getTagId(db, tagName1));
        Assertions.assertEquals(tagId1, tagId1FromDb);

        // Verify count of rows in metrics._tag for this tag name is exactly 1
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tag WHERE name = ?")) {
                statement.setString(1, tagName1);
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getInt(1));
                }
            }
            return null;
        });

        // 5. insert another distinct tag
        String tagName2 = "host_server1_" + System.nanoTime();
        Integer tagId2 = dbService.withBuilder(db -> handler.getTagId(db, tagName2));
        Assertions.assertNotNull(tagId2);
        Assertions.assertNotEquals(tagId1, tagId2);
    }

    @Test
    public void testConcurrentGetTagId() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        String commonTag = "concurrent_tag_" + System.nanoTime();
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Callable<Integer>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                IngestHandler handler = new IngestHandler();
                return dbService.withBuilder(db -> handler.getTagId(db, commonTag));
            });
        }

        List<Future<Integer>> futures = executor.invokeAll(tasks);
        Integer expectedId = null;
        for (Future<Integer> future : futures) {
            Integer id = future.get();
            Assertions.assertNotNull(id);
            if (expectedId == null) {
                expectedId = id;
            } else {
                Assertions.assertEquals(expectedId, id);
            }
        }
        executor.shutdown();

        // Verify only 1 row was inserted
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tag WHERE name = ?")) {
                statement.setString(1, commonTag);
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testGetTagIds() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();

        // 1. null or empty set returns empty map
        Map<String, Integer> nullResult = dbService.withBuilder(db -> handler.getTagIds(db, null));
        Assertions.assertNotNull(nullResult);
        Assertions.assertTrue(nullResult.isEmpty());

        Map<String, Integer> emptyResult = dbService.withBuilder(db -> handler.getTagIds(db, Collections.emptySet()));
        Assertions.assertNotNull(emptyResult);
        Assertions.assertTrue(emptyResult.isEmpty());

        Set<String> setWithNull = new HashSet<>();
        setWithNull.add(null);
        Map<String, Integer> nullElementResult = dbService.withBuilder(db -> handler.getTagIds(db, setWithNull));
        Assertions.assertNotNull(nullElementResult);
        Assertions.assertTrue(nullElementResult.isEmpty());

        // 2. insert batch of new tags
        String tag1 = "batch_tag_1_" + System.nanoTime();
        String tag2 = "batch_tag_2_" + System.nanoTime();
        String tag3 = "batch_tag_3_" + System.nanoTime();

        Set<String> batch1 = Set.of(tag1, tag2, tag3);
        Map<String, Integer> result1 = dbService.withBuilder(db -> handler.getTagIds(db, batch1));

        Assertions.assertNotNull(result1);
        Assertions.assertEquals(3, result1.size());
        Assertions.assertNotNull(result1.get(tag1));
        Assertions.assertNotNull(result1.get(tag2));
        Assertions.assertNotNull(result1.get(tag3));
        Assertions.assertTrue(result1.get(tag1) > 0);
        Assertions.assertTrue(result1.get(tag2) > 0);
        Assertions.assertTrue(result1.get(tag3) > 0);
        Assertions.assertEquals(3, new HashSet<>(result1.values()).size());

        // 3. fetch same tags again (cached)
        Map<String, Integer> result1Cached = dbService.withBuilder(db -> handler.getTagIds(db, batch1));
        Assertions.assertEquals(result1, result1Cached);

        // 4. mix of existing tags in DB + new tags + cached tags
        String tag4 = "batch_tag_4_" + System.nanoTime();
        String tag5 = "batch_tag_5_" + System.nanoTime();
        // pre-insert tag4 via separate handler instance (so it exists in DB but not in handler's cache)
        IngestHandler handler2 = new IngestHandler();
        Integer tag4Id = dbService.withBuilder(db -> handler2.getTagId(db, tag4));
        Assertions.assertNotNull(tag4Id);

        // handler has tag1 in cache, tag4 in DB (not in handler's cache), tag5 is completely new
        Set<String> mixedBatch = Set.of(tag1, tag4, tag5);
        Map<String, Integer> mixedResult = dbService.withBuilder(db -> handler.getTagIds(db, mixedBatch));

        Assertions.assertEquals(3, mixedResult.size());
        Assertions.assertEquals(result1.get(tag1), mixedResult.get(tag1));
        Assertions.assertEquals(tag4Id, mixedResult.get(tag4));
        Assertions.assertNotNull(mixedResult.get(tag5));
        Assertions.assertTrue(mixedResult.get(tag5) > 0);

        // 5. Verify single row in metrics._tag for each tag
        for (String tag : List.of(tag1, tag2, tag3, tag4, tag5)) {
            dbService.withDB(c -> {
                try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tag WHERE name = ?")) {
                    statement.setString(1, tag);
                    try (ResultSet rs = statement.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getInt(1));
                    }
                }
                return null;
            });
        }
    }

    @Test
    public void testConcurrentGetTagIds() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        String prefix = "concurrent_batch_" + System.nanoTime() + "_";
        Set<String> tags = Set.of(prefix + "1", prefix + "2", prefix + "3", prefix + "4", prefix + "5");

        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Callable<Map<String, Integer>>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                IngestHandler handler = new IngestHandler();
                return dbService.withBuilder(db -> handler.getTagIds(db, tags));
            });
        }

        List<Future<Map<String, Integer>>> futures = executor.invokeAll(tasks);
        Map<String, Integer> expected = null;
        for (Future<Map<String, Integer>> future : futures) {
            Map<String, Integer> res = future.get();
            Assertions.assertNotNull(res);
            Assertions.assertEquals(5, res.size());
            if (expected == null) {
                expected = res;
            } else {
                Assertions.assertEquals(expected, res);
            }
        }
        executor.shutdown();

        // Verify only 1 row was inserted for each tag
        for (String tag : tags) {
            dbService.withDB(c -> {
                try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tag WHERE name = ?")) {
                    statement.setString(1, tag);
                    try (ResultSet rs = statement.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getInt(1));
                    }
                }
                return null;
            });
        }
    }

    @Test
    public void testImportTagsBatch() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "batch_metric_" + System.nanoTime();
        List<List<String>> initialTags = List.of(
                List.of("host", "srv1"),
                List.of("env", "prod"),
                List.of("region", "us-east")
        );
        Pair<Table, Table> tables =
                handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        // 1. null / empty tagsList returns empty map
        Map<List<List<String>>, Integer> nullResult = dbService.withBuilder(db -> handler.importTags(db, null, fields, tagsTable));
        Assertions.assertNotNull(nullResult);
        Assertions.assertTrue(nullResult.isEmpty());

        Map<List<List<String>>, Integer> emptyResult = dbService.withBuilder(db -> handler.importTags(db, Collections.emptyList(), fields, tagsTable));
        Assertions.assertNotNull(emptyResult);
        Assertions.assertTrue(emptyResult.isEmpty());

        // 2. Insert batch of distinct tags
        List<List<String>> t1 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h1")), new ArrayList<>(List.of("env", "prod")), new ArrayList<>(List.of("region", "us-east"))));
        List<List<String>> t2 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h2")), new ArrayList<>(List.of("env", "stage")), new ArrayList<>(List.of("region", "us-west"))));
        List<List<String>> t3 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h3")), new ArrayList<>(List.of("env", "dev")))); // missing region (null)

        List<List<List<String>>> batch1 = List.of(t1, t2, t3);
        Map<List<List<String>>, Integer> result1 = dbService.withBuilder(db -> handler.importTags(db, batch1, fields, tagsTable));

        Assertions.assertNotNull(result1);
        Assertions.assertEquals(3, result1.size());
        Integer id1 = result1.get(t1);
        Integer id2 = result1.get(t2);
        Integer id3 = result1.get(t3);
        Assertions.assertNotNull(id1);
        Assertions.assertNotNull(id2);
        Assertions.assertNotNull(id3);
        Assertions.assertTrue(id1 > 0);
        Assertions.assertTrue(id2 > 0);
        Assertions.assertTrue(id3 > 0);
        Assertions.assertEquals(3, new HashSet<>(result1.values()).size());

        // Verify selectTags returns identical IDs
        dbService.withBuilder(db -> {
            Assertions.assertEquals(id1, handler.selectTags(db, t1, fields, tagsTable).id);
            Assertions.assertEquals(id2, handler.selectTags(db, t2, fields, tagsTable).id);
            Assertions.assertEquals(id3, handler.selectTags(db, t3, fields, tagsTable).id);
            return null;
        });

        // 3. Batch with duplicates within the same batch
        List<List<List<String>>> batchWithDuplicates = List.of(t1, t2, t1, t2, t1);
        Map<List<List<String>>, Integer> dupResult = dbService.withBuilder(db -> handler.importTags(db, batchWithDuplicates, fields, tagsTable));
        Assertions.assertEquals(2, dupResult.size());
        Assertions.assertEquals(id1, dupResult.get(t1));
        Assertions.assertEquals(id2, dupResult.get(t2));

        // 4. Mixed batch: existing tags + new tags on a fresh handler (so not in cache)
        IngestHandler handler2 = new IngestHandler();
        handler2.dbService = dbService;

        List<List<String>> t4 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h4")), new ArrayList<>(List.of("env", "prod"))));
        List<List<String>> t5 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h5")), new ArrayList<>(List.of("region", "eu-central"))));
        List<List<List<String>>> mixedBatch = List.of(t1, t4, t2, t5);

        Map<List<List<String>>, Integer> mixedResult = dbService.withBuilder(db -> handler2.importTags(db, mixedBatch, fields, tagsTable));
        Assertions.assertEquals(4, mixedResult.size());
        Assertions.assertEquals(id1, mixedResult.get(t1));
        Assertions.assertEquals(id2, mixedResult.get(t2));
        Integer id4 = mixedResult.get(t4);
        Integer id5 = mixedResult.get(t5);
        Assertions.assertNotNull(id4);
        Assertions.assertNotNull(id5);
        Assertions.assertTrue(id4 > 0);
        Assertions.assertTrue(id5 > 0);

        // Verify total row count in metrics._tags_<tableName> is exactly 5
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM " + tagsTable.getName())) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(5, rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testConcurrentImportTagsBatch() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "concurrent_tags_metric_" + System.nanoTime();
        List<List<String>> initialTags = List.of(
                List.of("host", "srv1"),
                List.of("env", "prod")
        );
        Pair<Table, Table> tables =
                handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        String prefix = "conc_host_" + System.nanoTime() + "_";

        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Callable<Map<List<List<String>>, Integer>>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                IngestHandler h = new IngestHandler();
                h.dbService = dbService;
                List<List<List<String>>> tagsList = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    tagsList.add(new ArrayList<>(List.of(
                            new ArrayList<>(List.of("host", prefix + (j % 5))),
                            new ArrayList<>(List.of("env", "env_" + (j % 3)))
                    )));
                }
                return dbService.withBuilder(db -> h.importTags(db, tagsList, fields, tagsTable));
            });
        }

        List<Future<Map<List<List<String>>, Integer>>> futures = executor.invokeAll(tasks);
        Map<List<List<String>>, Integer> expected = null;
        for (var future : futures) {
            Map<List<List<String>>, Integer> res = future.get();
            Assertions.assertNotNull(res);
            if (expected == null) {
                expected = res;
            } else {
                Assertions.assertEquals(expected, res);
            }
        }
        executor.shutdown();

        // Verify count of rows in _tags table matches expected distinct count
        Set<List<List<String>>> distinctSets = new HashSet<>();
        for (int j = 0; j < 10; j++) {
            List<List<String>> tags = new ArrayList<>(List.of(
                    new ArrayList<>(List.of("host", prefix + (j % 5))),
                    new ArrayList<>(List.of("env", "env_" + (j % 3)))
            ));
            tags.sort(Comparator.comparing(List::getFirst));
            distinctSets.add(tags);
        }

        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM " + tagsTable.getName())) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(distinctSets.size(), rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testNineColumnsRepeatedExecutions() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "metric_9cols_rep_" + System.nanoTime();
        List<List<String>> initialTags = new ArrayList<>();
        for (int c = 1; c <= 9; c++) {
            initialTags.add(List.of("tag_col_" + c, "init_val_" + c));
        }

        Pair<Table, Table> tables = handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        // Run 20 iterations of 10 tag combos x 9 columns
        for (int iter = 1; iter <= 20; iter++) {
            List<List<List<String>>> tagsList = new ArrayList<>();
            for (int r = 0; r < 10; r++) {
                List<List<String>> row = new ArrayList<>();
                for (int c = 1; c <= 9; c++) {
                    row.add(List.of("tag_col_" + c, "val_" + c + "_" + (iter * 100 + r)));
                }
                tagsList.add(row);
            }

            long start = System.currentTimeMillis();
            Map<List<List<String>>, Integer> res = dbService.withBuilder(db -> handler.importTags(db, tagsList, fields, tagsTable));
            long duration = System.currentTimeMillis() - start;
            System.out.println("Iteration " + iter + " took: " + duration + "ms (inserted " + res.size() + ")");
            Assertions.assertEquals(10, res.size());
        }
    }

    @Test
    public void testImportTags2Batch() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "batch2_metric_" + System.nanoTime();
        List<List<String>> initialTags = List.of(
                List.of("host", "srv1"),
                List.of("env", "prod"),
                List.of("region", "us-east")
        );
        Pair<Table, Table> tables =
                handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        // 1. null / empty tagsList returns empty map
        Map<List<List<String>>, Integer> nullResult = dbService.withBuilder(db -> handler.importTags2(db, null, fields, tagsTable));
        Assertions.assertNotNull(nullResult);
        Assertions.assertTrue(nullResult.isEmpty());

        Map<List<List<String>>, Integer> emptyResult = dbService.withBuilder(db -> handler.importTags2(db, Collections.emptyList(), fields, tagsTable));
        Assertions.assertNotNull(emptyResult);
        Assertions.assertTrue(emptyResult.isEmpty());

        // 2. Insert batch of distinct tags
        List<List<String>> t1 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h1")), new ArrayList<>(List.of("env", "prod")), new ArrayList<>(List.of("region", "us-east"))));
        List<List<String>> t2 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h2")), new ArrayList<>(List.of("env", "stage")), new ArrayList<>(List.of("region", "us-west"))));
        List<List<String>> t3 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h3")), new ArrayList<>(List.of("env", "dev")))); // missing region (null)

        List<List<List<String>>> batch1 = List.of(t1, t2, t3);
        Map<List<List<String>>, Integer> result1 = dbService.withBuilder(db -> handler.importTags2(db, batch1, fields, tagsTable));

        Assertions.assertNotNull(result1);
        Assertions.assertEquals(3, result1.size());
        Integer id1 = result1.get(t1);
        Integer id2 = result1.get(t2);
        Integer id3 = result1.get(t3);
        Assertions.assertNotNull(id1);
        Assertions.assertNotNull(id2);
        Assertions.assertNotNull(id3);
        Assertions.assertTrue(id1 > 0);
        Assertions.assertTrue(id2 > 0);
        Assertions.assertTrue(id3 > 0);
        Assertions.assertEquals(3, new HashSet<>(result1.values()).size());

        // Verify selectTags returns identical IDs
        dbService.withBuilder(db -> {
            Assertions.assertEquals(id1, handler.selectTags(db, t1, fields, tagsTable).id);
            Assertions.assertEquals(id2, handler.selectTags(db, t2, fields, tagsTable).id);
            Assertions.assertEquals(id3, handler.selectTags(db, t3, fields, tagsTable).id);
            return null;
        });

        // 3. Batch with duplicates within the same batch
        List<List<List<String>>> batchWithDuplicates = List.of(t1, t2, t1, t2, t1);
        Map<List<List<String>>, Integer> dupResult = dbService.withBuilder(db -> handler.importTags2(db, batchWithDuplicates, fields, tagsTable));
        Assertions.assertEquals(2, dupResult.size());
        Assertions.assertEquals(id1, dupResult.get(t1));
        Assertions.assertEquals(id2, dupResult.get(t2));

        // 4. Mixed batch: existing tags + new tags on a fresh handler (so not in cache)
        IngestHandler handler2 = new IngestHandler();
        handler2.dbService = dbService;

        List<List<String>> t4 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h4")), new ArrayList<>(List.of("env", "prod"))));
        List<List<String>> t5 = new ArrayList<>(List.of(new ArrayList<>(List.of("host", "h5")), new ArrayList<>(List.of("region", "eu-central"))));
        List<List<List<String>>> mixedBatch = List.of(t1, t4, t2, t5);

        Map<List<List<String>>, Integer> mixedResult = dbService.withBuilder(db -> handler2.importTags2(db, mixedBatch, fields, tagsTable));
        Assertions.assertEquals(4, mixedResult.size());
        Assertions.assertEquals(id1, mixedResult.get(t1));
        Assertions.assertEquals(id2, mixedResult.get(t2));
        Integer id4 = mixedResult.get(t4);
        Integer id5 = mixedResult.get(t5);
        Assertions.assertNotNull(id4);
        Assertions.assertNotNull(id5);
        Assertions.assertTrue(id4 > 0);
        Assertions.assertTrue(id5 > 0);

        // Verify total row count in metrics._tags_<tableName> is exactly 5
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM " + tagsTable.getName())) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(5, rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testConcurrentImportTags2Batch() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "concurrent_tags2_metric_" + System.nanoTime();
        List<List<String>> initialTags = List.of(
                List.of("host", "srv1"),
                List.of("env", "prod")
        );
        Pair<Table, Table> tables =
                handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        String prefix = "conc2_host_" + System.nanoTime() + "_";

        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Callable<Map<List<List<String>>, Integer>>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                IngestHandler h = new IngestHandler();
                h.dbService = dbService;
                List<List<List<String>>> tagsList = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    tagsList.add(new ArrayList<>(List.of(
                            new ArrayList<>(List.of("host", prefix + (j % 5))),
                            new ArrayList<>(List.of("env", "env_" + (j % 3)))
                    )));
                }
                return dbService.withBuilder(db -> h.importTags2(db, tagsList, fields, tagsTable));
            });
        }

        List<Future<Map<List<List<String>>, Integer>>> futures = executor.invokeAll(tasks);
        Map<List<List<String>>, Integer> expected = null;
        for (var future : futures) {
            Map<List<List<String>>, Integer> res = future.get();
            Assertions.assertNotNull(res);
            if (expected == null) {
                expected = res;
            } else {
                Assertions.assertEquals(expected, res);
            }
        }
        executor.shutdown();

        // Verify count of rows in _tags table matches expected distinct count
        Set<List<List<String>>> distinctSets = new HashSet<>();
        for (int j = 0; j < 10; j++) {
            List<List<String>> tags = new ArrayList<>(List.of(
                    new ArrayList<>(List.of("host", prefix + (j % 5))),
                    new ArrayList<>(List.of("env", "env_" + (j % 3)))
            ));
            tags.sort(Comparator.comparing(List::getFirst));
            distinctSets.add(tags);
        }

        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM " + tagsTable.getName())) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(distinctSets.size(), rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testNineColumnsRepeatedExecutionsImportTags2() throws Exception {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "metric_9cols_rep2_" + System.nanoTime();
        List<List<String>> initialTags = new ArrayList<>();
        for (int c = 1; c <= 9; c++) {
            initialTags.add(List.of("tag_col_" + c, "init_val_" + c));
        }

        Pair<Table, Table> tables = handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags));
        Table tagsTable = tables.value;
        List<Field> fields = tagsTable.getFields();

        // Run 20 iterations of 10 tag combos x 9 columns
        for (int iter = 1; iter <= 20; iter++) {
            List<List<List<String>>> tagsList = new ArrayList<>();
            for (int r = 0; r < 10; r++) {
                List<List<String>> row = new ArrayList<>();
                for (int c = 1; c <= 9; c++) {
                    row.add(List.of("tag_col_" + c, "val_" + c + "_" + (iter * 100 + r)));
                }
                tagsList.add(row);
            }

            long start = System.currentTimeMillis();
            Map<List<List<String>>, Integer> res = dbService.withBuilder(db -> handler.importTags2(db, tagsList, fields, tagsTable));
            long duration = System.currentTimeMillis() - start;
            System.out.println("Iteration " + iter + " (importTags2) took: " + duration + "ms (inserted " + res.size() + ")");
            Assertions.assertEquals(10, res.size());
        }
    }

    @Test
    public void testImportTagsVsImportTags2Equivalence() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler1 = new IngestHandler();
        handler1.dbService = dbService;

        IngestHandler handler2 = new IngestHandler();
        handler2.dbService = dbService;

        String tableName1 = "equiv_m1_" + System.nanoTime();
        String tableName2 = "equiv_m2_" + System.nanoTime();

        List<List<String>> initialTags = List.of(
                List.of("host", "srv1"),
                List.of("env", "prod"),
                List.of("dc", "us-east")
        );

        Table tagsTable1 = handler1.tablesCache.get(tableName1, tn -> handler1.createMetricTable(tableName1, initialTags)).value;
        Table tagsTable2 = handler2.tablesCache.get(tableName2, tn -> handler2.createMetricTable(tableName2, initialTags)).value;

        List<List<List<String>>> testBatch = List.of(
                List.of(List.of("host", "node1"), List.of("env", "prod"), List.of("dc", "us-east")),
                List.of(List.of("host", "node2"), List.of("env", "dev")),
                List.of(List.of("host", "node3"), List.of("dc", "eu-central")),
                List.of(List.of("env", "stage"))
        );

        Map<List<List<String>>, Integer> res1 = dbService.withBuilder(db -> handler1.importTags(db, testBatch, tagsTable1.getFields(), tagsTable1));
        Map<List<List<String>>, Integer> res2 = dbService.withBuilder(db -> handler2.importTags2(db, testBatch, tagsTable2.getFields(), tagsTable2));

        Assertions.assertEquals(res1.size(), res2.size());
        for (List<List<String>> combo : testBatch) {
            Assertions.assertNotNull(res1.get(combo));
            Assertions.assertNotNull(res2.get(combo));
        }

        // Test with existing combos + new combos on fresh handlers
        IngestHandler handler1Fresh = new IngestHandler();
        handler1Fresh.dbService = dbService;
        IngestHandler handler2Fresh = new IngestHandler();
        handler2Fresh.dbService = dbService;

        List<List<List<String>>> mixedBatch = List.of(
                List.of(List.of("host", "node1"), List.of("env", "prod"), List.of("dc", "us-east")), // existing
                List.of(List.of("host", "node4"), List.of("env", "prod")), // new
                List.of(List.of("host", "node2"), List.of("env", "dev")) // existing
        );

        Map<List<List<String>>, Integer> mixedRes1 = dbService.withBuilder(db -> handler1Fresh.importTags(db, mixedBatch, tagsTable1.getFields(), tagsTable1));
        Map<List<List<String>>, Integer> mixedRes2 = dbService.withBuilder(db -> handler2Fresh.importTags2(db, mixedBatch, tagsTable2.getFields(), tagsTable2));

        Assertions.assertEquals(3, mixedRes1.size());
        Assertions.assertEquals(3, mixedRes2.size());
        Assertions.assertEquals(res1.get(testBatch.get(0)), mixedRes1.get(mixedBatch.get(0)));
        Assertions.assertEquals(res2.get(testBatch.get(0)), mixedRes2.get(mixedBatch.get(0)));
    }

    @Test
    public void testImportTags2OrCombinationsSelectMatching() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String tableName = "or_match_metric_" + System.nanoTime();
        List<List<String>> initialTags = List.of(
                List.of("host", "default_host"),
                List.of("env", "default_env"),
                List.of("region", "default_region")
        );

        Table tagsTable = handler.tablesCache.get(tableName, tn -> handler.createMetricTable(tableName, initialTags)).value;
        List<Field> fields = tagsTable.getFields();

        List<List<String>> cAllNull = List.of();
        List<List<String>> cHostOnly = List.of(List.of("host", "h-only"));
        List<List<String>> cEnvOnly = List.of(List.of("env", "e-only"));
        List<List<String>> cHostEnv = List.of(List.of("host", "h-full"), List.of("env", "e-full"));
        List<List<String>> cAll = List.of(List.of("host", "h-all"), List.of("env", "e-all"), List.of("region", "r-all"));

        List<List<List<String>>> initialCombos = List.of(cAllNull, cHostOnly, cEnvOnly, cHostEnv, cAll);
        Map<List<List<String>>, Integer> inserted = dbService.withBuilder(db -> handler.importTags2(db, initialCombos, fields, tagsTable));

        Assertions.assertEquals(5, inserted.size());
        for (List<List<String>> c : initialCombos) {
            Assertions.assertNotNull(inserted.get(c));
            Assertions.assertTrue(inserted.get(c) > 0);
        }

        // Fresh handler to bypass all memory caches
        IngestHandler freshHandler = new IngestHandler();
        freshHandler.dbService = dbService;

        // Query with existing combos mixed with a new combo
        List<List<String>> cNew = List.of(List.of("host", "h-new"), List.of("region", "r-new"));
        List<List<List<String>>> queryCombos = List.of(cHostEnv, cNew, cAllNull, cAll, cEnvOnly, cHostOnly);

        Map<List<List<String>>, Integer> queried = dbService.withBuilder(db -> freshHandler.importTags2(db, queryCombos, fields, tagsTable));

        Assertions.assertEquals(6, queried.size());
        Assertions.assertEquals(inserted.get(cAllNull), queried.get(cAllNull));
        Assertions.assertEquals(inserted.get(cHostOnly), queried.get(cHostOnly));
        Assertions.assertEquals(inserted.get(cEnvOnly), queried.get(cEnvOnly));
        Assertions.assertEquals(inserted.get(cHostEnv), queried.get(cHostEnv));
        Assertions.assertEquals(inserted.get(cAll), queried.get(cAll));
        Assertions.assertNotNull(queried.get(cNew));
        Assertions.assertTrue(queried.get(cNew) > 0);
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
    public void testHandleMetricsSingleMetric() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String metricName = "test_cpu_load_" + System.nanoTime();
        long timestampNano = 1_700_000_000_123_000_000L;
        Timestamp expectedTimestamp = new Timestamp(1_700_000_000_123L);
        double expectedValue = 42.5;

        List<List<String>> tags = List.of(
                List.of("host", "server-1"),
                List.of("env", "production")
        );
        MetricData metricData = metric(metricName, expectedValue, timestampNano, tags);

        handler.handleMetrics(List.of(metricData));

        // 1. Verify in metrics.<tableName>
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT created_at, tags_id, value FROM metrics." + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(expectedTimestamp, rs.getTimestamp("created_at"));
                    Assertions.assertEquals(expectedValue, rs.getDouble("value"), 0.0001);
                    Assertions.assertTrue(rs.getLong("tags_id") > 0);
                    Assertions.assertFalse(rs.next());
                }
            }
            return null;
        });

        // 2. Verify view returns metric data with resolved tag names
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT value, created_at, host, env FROM " + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(expectedValue, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals(expectedTimestamp, rs.getTimestamp("created_at"));
                    Assertions.assertEquals("server-1", rs.getString("host"));
                    Assertions.assertEquals("production", rs.getString("env"));
                    Assertions.assertFalse(rs.next());
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsMultipleMetricsAndBatch() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String metricNameA = "test_metric_a_" + System.nanoTime();
        String metricNameB = "test_metric_b_" + System.nanoTime();

        List<List<String>> tagsA1 = List.of(List.of("host", "h1"), List.of("dc", "east"));
        List<List<String>> tagsA2 = List.of(List.of("host", "h2"), List.of("dc", "west"));
        List<List<String>> tagsB1 = List.of(List.of("service", "auth"));
        List<List<String>> tagsB2 = List.of(List.of("service", "billing"));

        MetricData a1 = metric(metricNameA, 10.0, 1_700_000_000_100_000_000L, tagsA1);
        MetricData a2 = metric(metricNameA, 20.0, 1_700_000_000_200_000_000L, tagsA1); // same tags as a1
        MetricData a3 = metric(metricNameA, 30.0, 1_700_000_000_300_000_000L, tagsA2); // different tags
        MetricData b1 = metric(metricNameB, 100.0, 1_700_000_000_400_000_000L, tagsB1);
        MetricData b2 = metric(metricNameB, 200.0, 1_700_000_000_500_000_000L, tagsB2);

        handler.handleMetrics(List.of(a1, a2, a3, b1, b2));

        // Verify metric A table rows and tags_id sharing
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT tags_id, value FROM metrics." + metricNameA + " ORDER BY value")) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    long tagsId1 = rs.getLong("tags_id");
                    Assertions.assertEquals(10.0, rs.getDouble("value"), 0.0001);

                    Assertions.assertTrue(rs.next());
                    long tagsId2 = rs.getLong("tags_id");
                    Assertions.assertEquals(20.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals(tagsId1, tagsId2, "Identical tags must share the same tags_id");

                    Assertions.assertTrue(rs.next());
                    long tagsId3 = rs.getLong("tags_id");
                    Assertions.assertEquals(30.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertNotEquals(tagsId1, tagsId3, "Different tags must have different tags_id");

                    Assertions.assertFalse(rs.next());
                }
            }
            return null;
        });

        // Verify metric B table rows
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics." + metricNameB)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getInt(1));
                }
            }
            return null;
        });

        // Verify querying view for metric B
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT value, service FROM " + metricNameB + " ORDER BY value")) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(100.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals("auth", rs.getString("service"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(200.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals("billing", rs.getString("service"));

                    Assertions.assertFalse(rs.next());
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsSchemaEvolutionAddTagColumns() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String metricName = "test_evolve_" + System.nanoTime();

        // Batch 1: single tag 'host'
        MetricData m1 = metric(metricName, 1.0, 1_700_000_001_000_000_000L, List.of(List.of("host", "srv1")));
        handler.handleMetrics(List.of(m1));

        // Batch 2: new tags 'region' and 'app' added
        MetricData m2 = metric(metricName, 2.0, 1_700_000_002_000_000_000L, List.of(
                List.of("host", "srv2"),
                List.of("region", "us-west"),
                List.of("app", "payments")
        ));
        handler.handleMetrics(List.of(m2));

        // Batch 3: partial tags (only 'region', host and app are null)
        MetricData m3 = metric(metricName, 3.0, 1_700_000_003_000_000_000L, List.of(
                List.of("region", "eu-central")
        ));
        handler.handleMetrics(List.of(m3));

        // Verify view returns all rows with correct schema evolution columns
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT value, host, region, app FROM " + metricName + " ORDER BY value")) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals("srv1", rs.getString("host"));
                    Assertions.assertNull(rs.getString("region"));
                    Assertions.assertNull(rs.getString("app"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals("srv2", rs.getString("host"));
                    Assertions.assertEquals("us-west", rs.getString("region"));
                    Assertions.assertEquals("payments", rs.getString("app"));

                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(3.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertNull(rs.getString("host"));
                    Assertions.assertEquals("eu-central", rs.getString("region"));
                    Assertions.assertNull(rs.getString("app"));

                    Assertions.assertFalse(rs.next());
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsTagsCachingAcrossBatches() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String metricName = "test_cache_" + System.nanoTime();

        List<List<String>> tags1 = List.of(List.of("host", "srv1"), List.of("env", "prod"));
        List<List<String>> tags2 = List.of(List.of("host", "srv2"), List.of("env", "prod"));
        List<List<String>> tags3 = List.of(List.of("host", "srv3"), List.of("env", "stage"));

        // Batch 1: insert with tags1 and tags2
        MetricData m1 = metric(metricName, 10.0, 1_700_000_001_000_000_000L, tags1);
        MetricData m2 = metric(metricName, 20.0, 1_700_000_002_000_000_000L, tags2);
        handler.handleMetrics(List.of(m1, m2));

        // Check tags count in _tags_ table
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getInt(1));
                }
            }
            return null;
        });

        // Batch 2: insert with cached tags1, cached tags2, and new tags3
        MetricData m3 = metric(metricName, 30.0, 1_700_000_003_000_000_000L, tags1); // cached
        MetricData m4 = metric(metricName, 40.0, 1_700_000_004_000_000_000L, tags2); // cached
        MetricData m5 = metric(metricName, 50.0, 1_700_000_005_000_000_000L, tags3); // new
        handler.handleMetrics(List.of(m3, m4, m5));

        // Verify total metrics rows is 5
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics." + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(5, rs.getInt(1));
                }
            }
            return null;
        });

        // Verify total tags combinations in _tags_ table is only 3
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT count(*) FROM metrics._tags_" + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(3, rs.getInt(1));
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsDefaultTimestampZero() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        String metricName = "test_ts_zero_" + System.nanoTime();
        long beforeTime = System.currentTimeMillis();

        MetricData metricData = metric(metricName, 99.9, 0L, List.of(List.of("host", "srv-ts")));
        handler.handleMetrics(List.of(metricData));

        long afterTime = System.currentTimeMillis();

        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT created_at, value FROM metrics." + metricName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Timestamp createdAt = rs.getTimestamp("created_at");
                    Assertions.assertNotNull(createdAt);
                    Assertions.assertTrue(createdAt.getTime() >= beforeTime - 1000);
                    Assertions.assertTrue(createdAt.getTime() <= afterTime + 1000);
                    Assertions.assertEquals(99.9, rs.getDouble("value"), 0.0001);
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsSpecialCharactersAndNormalization() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        // Metric name with dots and dashes
        String rawMetricName = "My-App.Http.Requests-Count." + System.nanoTime();
        String expectedTableName = rawMetricName.toLowerCase().replaceAll("\\W+", "_");

        // Tags with reserved word 'id' and hyphenated 'user-agent'
        MetricData metricData = metric(rawMetricName, 55.0, 1_700_000_000_000_000_000L, List.of(
                List.of("id", "req-1234"),
                List.of("user-agent", "Mozilla/5.0")
        ));

        handler.handleMetrics(List.of(metricData));

        // Verify query on view with normalized column names "_id" and "user_agent"
        dbService.withDB(c -> {
            try (PreparedStatement statement = c.prepareStatement("SELECT value, \"_id\", \"user_agent\" FROM " + expectedTableName)) {
                try (ResultSet rs = statement.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(55.0, rs.getDouble("value"), 0.0001);
                    Assertions.assertEquals("req-1234", rs.getString("_id"));
                    Assertions.assertEquals("Mozilla/5.0", rs.getString("user_agent"));
                }
            }
            return null;
        });
    }

    @Test
    public void testHandleMetricsEmptyBatch() {
        DBService dbService = DependencyFactory.get(DBService.class);
        IngestHandler handler = new IngestHandler();
        handler.dbService = dbService;

        Assertions.assertDoesNotThrow(() -> handler.handleMetrics(Collections.emptyList()));
    }
}
