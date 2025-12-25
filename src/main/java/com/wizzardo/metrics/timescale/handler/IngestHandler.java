package com.wizzardo.metrics.timescale.handler;

import com.wizzardo.http.HttpConnection;
import com.wizzardo.http.RestHandler;
import com.wizzardo.http.framework.di.Injectable;
import com.wizzardo.http.framework.di.PostConstruct;
import com.wizzardo.http.request.Request;
import com.wizzardo.http.response.Response;
import com.wizzardo.http.response.Status;
import com.wizzardo.metrics.timescale.db.generated.Tables;
import com.wizzardo.metrics.timescale.db.generated.TagTable;
import com.wizzardo.metrics.timescale.db.model.Metric;
import com.wizzardo.metrics.timescale.db.model.Tag;
import com.wizzardo.metrics.timescale.misc.InsertFieldsStep;
import com.wizzardo.metrics.timescale.model.MetricData;
import com.wizzardo.metrics.timescale.service.DBService;
import com.wizzardo.tools.cache.Cache;
import com.wizzardo.tools.interfaces.Mapper;
import com.wizzardo.tools.json.JsonTools;
import com.wizzardo.tools.misc.Pair;
import com.wizzardo.tools.misc.With;
import com.wizzardo.tools.sql.query.Condition;
import com.wizzardo.tools.sql.query.Field;
import com.wizzardo.tools.sql.query.QueryBuilder;
import com.wizzardo.tools.sql.query.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Injectable
public class IngestHandler extends RestHandler implements PostConstruct {
    DBService dbService;
    String schema = "metrics";
//    String schema = "public";

    record TagsCacheKey(
            String name,
            List<List<String>> tags
    ) {
    }

    Cache<TagsCacheKey, Integer> tagsCache = new Cache<>(3600);
    Cache<String, Integer> tagCache = new Cache<>(3600);
    Cache<String, String> stringCache = new Cache<>(-1, s -> s);
    Cache<String, Pair<Table, Table>> tablesCache = new Cache<>(-1);

    public IngestHandler() {
        super(IngestHandler.class.getSimpleName());
        post(this::handlePost);
    }

    public static class PgTable {
        public String tablename;
    }

    public static class HyperTable {
        public String hypertable_schema;
        public String hypertable_name;
    }

    public static class RetentionJob {
        public int job_id;
        public String hypertable_schema;
        public String hypertable_name;
        public RetentionConfig config;
    }

    public static class RetentionConfig {
        public String drop_after;
    }

    public static class Column {
        public String column_name;
        public String data_type;
        public String udt_name;
    }

    public static class IntIdHolder {
        public int id;
    }

    @Override
    public void init() {
        dbService.withBuilder(b -> {
//            SELECT *  FROM pg_catalog.pg_tables WHERE schemaname = 'metrics' ;
            Table pg_tables = new SimpleTable("pg_catalog.pg_tables");
            List<PgTable> tables = b.select(new Field(pg_tables, "tablename"))
                    .from(pg_tables)
                    .where(new Field.StringField(pg_tables, "schemaname").eq(schema))
                    .fetchInto(PgTable.class);

            System.out.println("tables:");
            for (PgTable table : tables) {
                if (table.tablename.startsWith("_tags_"))
                    continue;
                if (table.tablename.startsWith("_tag"))
                    continue;

                System.out.println(table.tablename);

                Table metricTable = new SimpleTable(schema + "." + table.tablename, t -> Arrays.asList(
                        new Field.DateField(t, "created_at"),
                        new Field.IntField(t, "tags_id"),
                        new Field.DoubleField(t, "value")
                ));

//                SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'metric';
                String tagsTableName = "_tags_" + table.tablename;
                Table metricTagsTable = getTagsTable(b, schema, tagsTableName);

                createMetricViewTable(table.tablename, metricTagsTable);

                tablesCache.put(table.tablename, Pair.of(metricTable, metricTagsTable));
            }

            enableCompression(b);
            enableRetention(b);

            return null;
        });
    }

    private void enableCompression(QueryBuilder.WrapConnectionStep b) throws SQLException {
        Table hypertablesTable = new SimpleTable("timescaledb_information.hypertables");
        List<HyperTable> tables = b.select(
                        new Field(hypertablesTable, "hypertable_schema"),
                        new Field(hypertablesTable, "hypertable_name")
                )
                .from(hypertablesTable)
                .where(new Field.BooleanField(hypertablesTable, "compression_enabled").eq(false))
                .fetchInto(HyperTable.class);

        for (HyperTable table : tables) {
            try {
                dbService.withDBTransaction(c -> {
                    enableCompression(c, table.hypertable_schema, table.hypertable_name);
                    return Void.TYPE;
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void enableCompression(Connection c, String hypertable_schema, String hypertable_name) throws SQLException {
        String sql = "ALTER TABLE " + hypertable_schema + "." + hypertable_name + " SET (timescaledb.compress,\n" +
                     "                timescaledb.compress_orderby = 'created_at DESC',\n" +
                     "                timescaledb.compress_segmentby = 'tags_id'\n" +
                     ")";
        System.out.println(sql);
        c.createStatement().execute(sql);

        sql = "SELECT add_compression_policy('" + hypertable_schema + "." + hypertable_name + "', compress_after => INTERVAL '2h')";
        System.out.println(sql);
        c.createStatement().execute(sql);
    }

    private void enableRetention(QueryBuilder.WrapConnectionStep b) throws SQLException {
        Table hypertablesTable = new SimpleTable("timescaledb_information.hypertables");
        List<HyperTable> tables = b.select(
                        new Field(hypertablesTable, "hypertable_schema"),
                        new Field(hypertablesTable, "hypertable_name")
                )
                .from(hypertablesTable)
                .fetchInto(HyperTable.class);

        Table jobsTable = new SimpleTable("timescaledb_information.jobs");

        List<RetentionJob> retentionJobs = b.select(
                        new Field(jobsTable, "job_id"),
                        new Field(jobsTable, "hypertable_schema"),
                        new Field(jobsTable, "hypertable_name"),
                        new Field(jobsTable, "config")
                )
                .from(jobsTable)
                .where(new Field.StringField(jobsTable, "proc_name").eq("policy_retention"))
                .fetchInto(RetentionJob.class);

        Map<Pair<String, String>, RetentionConfig> policies = new HashMap<>(tables.size());
        for (RetentionJob retentionJob : retentionJobs) {
            policies.put(Pair.of(retentionJob.hypertable_schema, retentionJob.hypertable_name), retentionJob.config);
        }

        String retention = "14 days";

        for (HyperTable table : tables) {
            RetentionConfig existingPolicy = policies.get(Pair.of(table.hypertable_schema, table.hypertable_name));
            System.out.println("retention config for " + table.hypertable_schema + "." + table.hypertable_name + ": " + (existingPolicy != null ? existingPolicy.drop_after : null));
            if (existingPolicy != null && retention.equals(existingPolicy.drop_after)) {
                continue;
            }

            dbService.withDBTransaction(c -> {
                if (existingPolicy != null) {
                    String sql = "SELECT remove_retention_policy('" + table.hypertable_schema + "." + table.hypertable_name + "');";
                    System.out.println(sql);
                    c.createStatement().execute(sql);
                }

                String sql = "SELECT add_retention_policy('" + table.hypertable_schema + "." + table.hypertable_name + "', drop_after => INTERVAL '" + retention + "');";
                System.out.println(sql);
                c.createStatement().execute(sql);
                return Void.TYPE;
            });

        }
    }

    private Table getTagsTable(QueryBuilder.WrapConnectionStep b, String schema, String tagsTableName) throws SQLException {
        Table columnsTable = new SimpleTable("information_schema.columns");
        List<Column> columns = b.select(
                        new Field(columnsTable, "column_name"),
                        new Field(columnsTable, "data_type"),
                        new Field(columnsTable, "udt_name")
                )
                .from(columnsTable)
                .where(new Field.StringField(columnsTable, "table_schema").eq(schema)
                        .and(new Field.StringField(columnsTable, "table_name").eq(tagsTableName))
                )
                .fetchInto(Column.class);


        Table metricTagsTable = new SimpleTable(schema + "." + tagsTableName, table -> With.map(new ArrayList<Field>(columns.size() + 1), it -> {
            for (Column column : columns) {
                switch (column.udt_name) {
                    case "int4" -> it.add(new Field.IntField(table, column.column_name));
                    case "text" -> it.add(new Field.StringField(table, "\"" + column.column_name + "\""));
                }
            }
            return it;
        }));

        for (Column column : columns) {
            System.out.println("\t" + column.column_name + ": " + column.udt_name);
        }
        return metricTagsTable;
    }

    private void createMetricViewTable(String tableName, Table tagsTable) {
        try {
            dbService.withDBTransaction(c -> {
                createMetricViewTable(c, tableName, tagsTable);
                return Void.TYPE;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createMetricViewTable(Connection c, String tableName, Table tagsTable) throws SQLException {
        String sql = "drop view if exists " + tableName;
        System.out.println(sql);
        c.createStatement().execute(sql);

        StringBuilder sb = new StringBuilder()
                .append("create view ").append(tableName).append(" as SELECT")
                .append(" m.value,")
                .append(" m.created_at");

        List<Field> fields = tagsTable.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if(f.getName().equals("id"))
                continue;

            sb.append(", ");
//            sb.append(" tags.").append(f.getName());
            sb.append("\"").append(f.getName()).append("\".name as \"").append(f.getName()).append("\"");
        }
        sb.append(" FROM ").append(schema).append(".").append(tableName).append(" m");
        sb.append(" join ").append(schema).append("._tags_").append(tableName).append(" tags on m.tags_id=tags.id");

        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if(f.getName().equals("id"))
                continue;
            String tag = f.getName();
            sb.append(" left join ").append(schema).append("._tag ")
                    .append("\"").append(tag).append("\" on tags.").append(tag)
                    .append(" = \"").append(tag).append("\".id");
        }

        System.out.println(sb);

        PreparedStatement statement = c.prepareStatement(sb.toString());
        statement.execute();
    }

    protected Response handlePost(Request<HttpConnection, Response> request, Response response) {
        List<MetricData> metricData = JsonTools.parse(request.getBody().bytes(), List.class, MetricData.class);
//        for (MetricData metric : metricData) {
//            insert(metric);
//        }

        long startPreparing = System.nanoTime();
        int[] createdTags = new int[]{0};
        int[] notCachedTags = new int[]{0};
        Map<String, List<Metric>> metrics = metricData.stream()
                .map(data -> {
                    Metric metric = new Metric();
                    metric.value = data.value;
                    if (data.timestamp != 0)
                        metric.createdAt = new Timestamp(data.timestamp / 1000_000);
                    else
                        metric.createdAt = new Timestamp(System.currentTimeMillis());

                    List<List<String>> tags = data.tags;
                    String tableName = toTableName(data.name);
                    Pair<Table, Table> tables = tablesCache.get(tableName, tn -> createMetricTable(tableName, tags));
                    GetTagsResult tagsResult = getTags(data, tables);
                    metric.tagsId = tagsResult.id;
                    if (!tagsResult.cached)
                        notCachedTags[0]++;
                    if (tagsResult.created)
                        createdTags[0]++;

                    return Pair.of(tableName, metric);
                })
                .collect(Collectors.groupingBy(
                        p -> p.key,
                        Collectors.mapping(p -> p.value, Collectors.toList())
                ));

        long stopPreparing = System.nanoTime();

        StringBuilder sb = new StringBuilder(256);
        sb.append("p ").append(createdTags[0]).append(" ").append(notCachedTags[0]).append(":").append((stopPreparing - startPreparing) / 1000f / 1000f).append("ms ");

        for (Map.Entry<String, List<Metric>> entry : metrics.entrySet()) {
            String key = entry.getKey();
            List<Metric> value = entry.getValue();
            long startInserting = System.nanoTime();
            insert(key, value);
            long stopInserting = System.nanoTime();
            sb.append("i ").append(value.size()).append(": ").append((stopInserting - startInserting) / 1000f / 1000f).append("ms ");
        }

        System.out.println(sb);

        return response.setStatus(Status._200).body("");
    }

    void insert(String tableName, List<Metric> metrics) {
        try {
            metrics.sort(Comparator.comparing(metric -> metric.createdAt));
            dbService.withDB(c -> {
                PreparedStatement statement = c.prepareStatement("INSERT INTO " + schema + "." + tableName + " (created_at, tags_id, value) VALUES (?, ?, ?)");
                for (int i = 0; i < metrics.size(); i++) {
                    Metric metric = metrics.get(i);
                    statement.setTimestamp(1, metric.createdAt);
                    statement.setLong(2, metric.tagsId);
                    statement.setDouble(3, metric.value);

                    statement.addBatch();
//                    if (i % 100 == 0) {
//                        statement.executeBatch();
//                    }
                }
                statement.executeBatch();
                return Void.TYPE;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Pair<Table, Table> createMetricTable(String tableName, List<List<String>> initialTags) {
        return dbService.withDB(c -> {
            c.setAutoCommit(false);
            try {
                PreparedStatement statement = c.prepareStatement(
                        "create table " + schema + "." + tableName + " (\n" +
                        "        created_at     TIMESTAMPTZ      NOT NULL,\n" +
                        "        tags_id        INTEGER          NOT NULL,\n" +
                        "        value          DOUBLE PRECISION NOT NULL\n" +
                        ")"
                );

                statement.execute();

                String tagsTableName = "_tags_" + tableName;
                String tagsTableNameWithSchema = schema+"._tags_" + tableName;
                StringBuilder sb = new StringBuilder()
                        .append("create table ").append(tagsTableNameWithSchema).append(" (\n")
                        .append("id SERIAL PRIMARY KEY,\n");

                for (int i = 0; i < initialTags.size(); i++) {
                    List<String> tag = initialTags.get(i);
                    if (i > 0)
                        sb.append(",\n");
                    sb.append('"').append(toColumnName(tag.get(0))).append("\" int");
                }
                sb.append(")");

                System.out.println(sb);

                statement = c.prepareStatement(sb.toString());
                statement.execute();

                statement = c.prepareStatement("CREATE INDEX " + tableName + "_created_at_tags_idx ON " + schema + "." + tableName + "(created_at DESC, tags_id)");
                statement.execute();

                statement = c.prepareStatement("SELECT create_hypertable('" + schema + "." + tableName + "', by_range('created_at'))");
                statement.execute();


                for (int i = 0; i < initialTags.size(); i++) {
                    List<String> tag = initialTags.get(i);
                    String columnName = toColumnName(tag.get(0));

                    String sql = "create index " + tagsTableName + "__" + columnName + " on " + tagsTableNameWithSchema + "(\"" + columnName + "\")";
                    System.out.println(sql);
                    statement = c.prepareStatement(sql);
                    statement.execute();
                }

                Table metricTable = new SimpleTable(schema + "." + tableName, table -> Arrays.asList(
                        new Field.DateField(table, "created_at"),
                        new Field.IntField(table, "tags_id"),
                        new Field.DoubleField(table, "value")
                ));

                Table metricTagsTable = getTagsTable(QueryBuilder.withConnection(c), schema, tagsTableName);

                createMetricViewTable(c, tableName, metricTagsTable);
                enableCompression(c, schema, tableName);

                c.commit();

                return Pair.of(metricTable, metricTagsTable);
            } catch (Exception e) {
                e.printStackTrace();
                c.rollback();
                throw new RuntimeException(e);
            } finally {
                c.setAutoCommit(true);
            }
        });
    }

    private String toTableName(String name) {
        return name.toLowerCase().replaceAll("\\W+", "_");
    }

    private String toColumnName(String name) {
        name = name.toLowerCase();
        if (name.equals("id"))
            return "_id";
        return name.replaceAll("\\W+", "_");
    }

    public static class TagIdHolder {
        public int id;
    }

    static class GetTagsResult {
        Integer id;
        boolean cached = true;
        boolean created;
    }

    GetTagsResult getTags(MetricData metricData, Pair<Table, Table> metricTables) {
        internStrings(metricData.tags);
        metricData.tags.sort(Comparator.comparing(List::getFirst));
        TagsCacheKey key = new TagsCacheKey(metricTables.key.getName(), metricData.tags);
        GetTagsResult result = new GetTagsResult();
        result.id = tagsCache.get(key, s ->
                        dbService.withBuilder(db -> {
                            result.cached = false;
                            Table tagsTable = metricTables.value;

                            if (metricData.tags.stream().anyMatch(kv -> metricTables.value.getFields().stream().noneMatch(field -> isColumnMatchingTag(field, toColumnName(kv.get(0)))))) {
                                tagsTable = updateColumns(metricTables, metricData.tags);
                            }

                            List<Field> fields = tagsTable.getFields();

                            Condition condition = Condition.TRUE_CONDITION;
                            for (int i = 1; i < fields.size(); i++) {
                                Field field = fields.get(i);
                                String value = null;
                                for (int j = 0; j < metricData.tags.size(); j++) {
                                    List<String> kv = metricData.tags.get(j);
                                    if (isColumnMatchingTag(field, toColumnName(kv.get(0)))) {
                                        value = kv.get(1);
                                        break;
                                    }
                                }

                                Integer tagId = getTagId(db, value);
                                condition = condition.and(((Field.IntField) field).eq(tagId));
                            }


                            QueryBuilder.WhereStep q = db.select(fields.get(0))
                                    .from(tagsTable)
                                    .where(condition);
//                    System.out.println(q.toSql());
                            TagIdHolder idHolder = q.fetchOneInto(TagIdHolder.class);

                            if (idHolder == null) {
                                List<Field> tagsColumns = fields.subList(1, fields.size());
                                QueryBuilder.InsertValuesStep query = new QueryBuilder.InsertValuesStep(
//                                        db.insertInto(tagsTable).fields(tagsColumns),
                                        new InsertFieldsStep(db.insertInto(tagsTable), tagsColumns),
                                        metricData.tags,
                                        tagsColumns.stream().map(field -> (Field.ToSqlMapper) (o, builder) -> {
                                            for (int j = 0; j < metricData.tags.size(); j++) {
                                                List<String> kv = metricData.tags.get(j);
                                                if (isColumnMatchingTag(field, toColumnName(kv.get(0)))) {
                                                    Integer tagId = getTagId(db, kv.get(1));
                                                    builder.setField(tagId);
                                                    return;
                                                }
                                            }
                                            builder.setField((Integer) null);
                                        }).toList()
                                );

//                                System.out.println(query.toSql());

                                int id = (int) query.executeInsert(fields.get(0));
                                result.created = true;
                                return id;
                            }
                            return idHolder.id;
                        })
        );
        return result;
    }

    private Integer getTagId(QueryBuilder.WrapConnectionStep db, String value) {
        if (value == null) {
            return null;
        }
        return tagCache.get(value, name -> {
            TagTable tag = new TagTable("metrics._tag", null);
            QueryBuilder.ReturningStep query = db.insertInto(tag)
                    .values(With.with(new Tag(), it -> it.name = name))
                    .onConflictDoUpdate(tag.NAME)
                    .set(tag.NAME.eq(name))
                    .returning(tag.ID);
//            System.out.println(query.toSql());
            return query
                    .fetchOneInto(IntIdHolder.class).id;
//                                    return db.select(tagTable.FIELDS).from(tagTable).where(tagTable.NAME.eq(name)).fetchOneInto(Tag.class).id;
        });
    }

    private void internStrings(List<List<String>> tags) {
        for (int i = 0; i < tags.size(); i++) {
            List<String> kv = tags.get(i);
            kv.set(0, stringCache.get(kv.get(0)));
            kv.set(1, stringCache.get(kv.get(1)));
        }
    }

    private Table updateColumns(Pair<Table, Table> metricTables, List<List<String>> tags) {
        return dbService.withDB(c -> {
            c.setAutoCommit(false);
            Table tagsTable = metricTables.value;
            Table metricTagsTable;
            System.out.println("updateColumns to match tags: " + tags);
            String tableName = metricTables.key.getName().split("\\.")[1];
            String tagsTableName = metricTables.value.getName().split("\\.")[1];
            try {
                for (List<String> kv : tags) {
                    String columnName = toColumnName(kv.get(0));
                    if (tagsTable.getFields().stream().anyMatch(field -> isColumnMatchingTag(field, columnName)))
                        continue;

                    {
                        String sql = "alter table " + tagsTable.getName() + " add column \"" + columnName + "\" int";
                        System.out.println(sql);
                        PreparedStatement statement = c.prepareStatement(sql);
                        statement.execute();
                    }
                    {
                        String sql = "create index " + tagsTableName + "__" + columnName + " on " + tagsTable.getName() + "(\"" + columnName + "\")";
                        System.out.println(sql);
                        PreparedStatement statement = c.prepareStatement(sql);
                        statement.execute();
                    }
                }

                metricTagsTable = getTagsTable(QueryBuilder.withConnection(c), schema, tagsTable.getName().split("\\.")[1]);
                createMetricViewTable(c, tableName, metricTagsTable);

                c.commit();
            } catch (Exception e) {
                e.printStackTrace();
                c.rollback();
                throw new RuntimeException(e);
            } finally {
                c.setAutoCommit(true);
            }


            Pair<Table, Table> tablePair = Pair.of(metricTables.key, metricTagsTable);
            tablesCache.put(tableName, tablePair);
            return metricTagsTable;
        });
    }

    private static boolean isColumnMatchingTag(Field field, String columnName) {
        int i = field.getName().indexOf(columnName);
        return (i == 1 && field.getName().length() == columnName.length() + 2) || (i == 0 && field.getName().length() == columnName.length());
    }

    static class SimpleTable extends Table {
        protected List<Field> fields;

        public SimpleTable(String name) {
            super(name);
            fields = Collections.emptyList();
        }

        public SimpleTable(String name, Mapper<Table, List<Field>> fieldCreator) {
            super(name);
            this.fields = fieldCreator.map(this);
        }

        @Override
        public List<Field> getFields() {
            return fields;
        }
    }
}
