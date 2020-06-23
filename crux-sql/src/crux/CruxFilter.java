package crux.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.util.List;
import java.util.Map;

public class CruxFilter extends Filter implements CruxRel {
    private final IFn filterFn;

    CruxFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() == CruxRel.CONVENTION;
        assert getConvention() == child.getConvention();
        this.filterFn = CruxUtils.resolveWithErrorLogging("crux.calcite/enrich-filter");
    }

    @Override public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
        return new CruxFilter(getCluster(), relTraitSet, input, condition);
    }

    @SuppressWarnings("unchecked")
    @Override public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.schema = (Map<Keyword, Object>) filterFn.invoke(implementor.schema, condition);
    }
}
