package crux.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import java.util.List;

public class CruxFilter extends Filter implements CruxRel {
    CruxFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() == CruxRel.CONVENTION;
        assert getConvention() == child.getConvention();
    }

    @Override public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
        return new CruxFilter(getCluster(), relTraitSet, input, condition);
    }

    @SuppressWarnings("unchecked")
    @Override public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        List<String> newClauses = (List<String>) CruxUtils.resolve("crux.calcite/filter->clause").invoke(implementor.cruxTable.schema, condition);
        implementor.clauses.addAll(newClauses);
    }
}
