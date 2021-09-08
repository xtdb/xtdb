package xtdb.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptTable;

import java.util.Map;

import clojure.lang.Keyword;

public interface XtdbRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("XTDB", XtdbRel.class);

    class Implementor {
        RelOptTable table;
        Map<Keyword, Object> schema;

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((XtdbRel) input).implement(this);
        }
    }
}
