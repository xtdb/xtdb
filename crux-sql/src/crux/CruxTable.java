package crux.calcite;

import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import crux.api.ICruxAPI;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import java.util.Map;
import java.util.List;
import clojure.lang.Keyword;

public class CruxTable extends AbstractTable implements ProjectableFilterableTable {
    private ICruxAPI node;
    private Map<Keyword, Object> schema;

    public CruxTable (ICruxAPI node, Map<Keyword, Object> schema) {
        this.node = node;
        this.schema = schema;
    }

    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return (RelDataType)resolve("crux.calcite/row-type").invoke(typeFactory, node, schema);
    }

    @SuppressWarnings("unchecked")
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        return (Enumerable<Object[]>) resolve("crux.calcite/scan").invoke(node, schema, root, filters, projects);
    }
}
