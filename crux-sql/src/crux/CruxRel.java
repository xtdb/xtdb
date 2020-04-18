package crux.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptTable;

import java.util.List;
import java.util.ArrayList;

public interface CruxRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("CRUX", CruxRel.class);

    class Implementor {
        RelOptTable table;
        CruxTable cruxTable;
        final List<Object> clauses = new ArrayList<>();

        public void add(Object clause) {
            clauses.add(clause);
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((CruxRel) input).implement(this);
        }
    }

}
