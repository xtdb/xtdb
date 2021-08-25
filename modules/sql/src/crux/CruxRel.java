package crux.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.Pair;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import clojure.lang.Keyword;

public interface CruxRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("CRUX", CruxRel.class);

    class Implementor {
        RelOptTable table;
        Map<Keyword, Object> schema;

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((CruxRel) input).implement(this);
        }
    }
}
