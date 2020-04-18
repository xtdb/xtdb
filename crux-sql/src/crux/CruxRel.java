package crux.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptTable;

public interface CruxRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("CRUX", CruxRel.class);

    class Implementor {
        //        final List<Pair<String, String>> list = new ArrayList<>();

        RelOptTable table;
        CruxTable cruxTable;

        // public void add(String findOp, String aggOp) {
        //     list.add(Pair.of(findOp, aggOp));
        // }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((CruxRel) input).implement(this);
        }
    }

}
