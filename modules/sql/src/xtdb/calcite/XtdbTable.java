package xtdb.calcite;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptCluster;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.util.Map;
import java.util.List;

public class XtdbTable extends AbstractQueryableTable implements TranslatableTable {
    private final Object node;
    public final Map<Keyword, Object> schema;
    private final IFn scanFn;
    private final boolean scanOnly;

    public XtdbTable(Object node, Map<Keyword, Object> schema, boolean scanOnly) {
        super(Object[].class);
        this.node = node;
        this.schema = schema;
        this.scanFn = XtdbUtils.resolve("xtdb.calcite/scan");
        this.scanOnly = scanOnly;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return (RelDataType) XtdbUtils.resolve("xtdb.calcite/row-type").invoke(typeFactory, node, schema);
    }

    @SuppressWarnings("unchecked")
    public <R> Enumerable<Object> find(Object schema, List<Class<?>> columnTypes, DataContext context) {
        return (Enumerable<Object>) scanFn.invoke(node, schema, columnTypes, context);
    }

    @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new XtdbQueryable<>(queryProvider, schema, this, tableName);
    }

    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new XtdbTableScan(cluster, cluster.traitSetOf(XtdbRel.CONVENTION), relOptTable, this, null, scanOnly);
    }

    public static class XtdbQueryable<T> extends AbstractTableQueryable<T> {
        XtdbQueryable(QueryProvider queryProvider, SchemaPlus schema,
                      XtdbTable table, String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            return null;
        }

        private XtdbTable getTable() {
            return (XtdbTable) table;
        }

        public <R> Enumerable<Object> find(Object schema, List<Class<?>> columnTypes, DataContext context) {
            return getTable().find(schema, columnTypes, context);
        }
    }
}
