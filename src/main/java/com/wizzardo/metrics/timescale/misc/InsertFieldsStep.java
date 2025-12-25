package com.wizzardo.metrics.timescale.misc;

import com.wizzardo.tools.sql.query.Field;
import com.wizzardo.tools.sql.query.FieldMappers;
import com.wizzardo.tools.sql.query.QueryBuilder;

import java.util.List;
import java.util.stream.Collectors;

public class InsertFieldsStep extends QueryBuilder.AbstractChainStep {
    private final List<Field> fields;

    public InsertFieldsStep(QueryBuilder.AbstractChainStep previous, List<Field> fields) {
        super(previous);
        this.fields = fields;
    }

    @Override
    public void toSql(QueryBuilder sb) {
        super.toSql(sb);
        sb.append(" (");
        for (Field field : fields) {
            sb.append("\"").append(field.getName()).append("\", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(")");
    }

    public QueryBuilder.InsertValuesStep values(Object o) {
        List<Field.ToSqlMapper> mappers = fields.stream().map(it -> FieldMappers.toSqlMapper(o.getClass(), it)).collect(Collectors.toList());
        return new QueryBuilder.InsertValuesStep(this, o, mappers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!super.equals(o)) return false;

        InsertFieldsStep that = (InsertFieldsStep) o;

        return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + fields.hashCode();
        return result;
    }
}