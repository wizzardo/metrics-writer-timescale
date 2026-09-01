package com.wizzardo.metrics.timescale.handler;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.IntegrationTestBase;
import com.wizzardo.metrics.timescale.service.DBService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
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
}
