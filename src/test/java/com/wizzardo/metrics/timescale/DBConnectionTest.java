package com.wizzardo.metrics.timescale;

import com.wizzardo.http.framework.di.DependencyFactory;
import com.wizzardo.metrics.timescale.service.DBService;
import com.wizzardo.tools.sql.query.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DBConnectionTest extends IntegrationTestBase {
    static public class TestValue {
        public int value;
    }

    @Test
    public void testConnection() {
        DBService dbService = DependencyFactory.get(DBService.class);
        TestValue testValue = dbService.withBuilder(db -> db.select(new Field("42").as("value")).fetchOneInto(TestValue.class));
        Assertions.assertEquals(42, testValue.value);
    }


}
