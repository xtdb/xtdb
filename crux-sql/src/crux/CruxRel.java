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

public interface CruxRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("CRUX", CruxRel.class);

    class Implementor {
        RelOptTable table;
        CruxTable cruxTable;
        int offset = 0;
        int fetch = -1;
        final List<Map.Entry<Integer, RelFieldCollation.Direction>> sort = new ArrayList<>();
        final List<Object> clauses = new ArrayList<>();

        public void add(Object clause) {
            clauses.add(clause);
        }

        public void addSort(Integer field, RelFieldCollation.Direction direction) {
            Objects.requireNonNull(field, "field");
            sort.add(new Pair<>(field, direction));
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((CruxRel) input).implement(this);
        }
    }
}
