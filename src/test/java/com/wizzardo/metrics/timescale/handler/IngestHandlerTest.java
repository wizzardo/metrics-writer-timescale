package com.wizzardo.metrics.timescale.handler;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.IntegrationTestBase;
import com.wizzardo.metrics.timescale.service.DBService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
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
}
