package xtdb.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import clojure.lang.Keyword;
import clojure.lang.IFn;

import java.util.Map;

public class XtdbFilter extends Filter implements XtdbRel {
    private final IFn filterFn;

    XtdbFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() == XtdbRel.CONVENTION;
        assert getConvention() == child.getConvention();
        this.filterFn = XtdbUtils.resolveWithErrorLogging("xtdb.calcite/enrich-filter");
    }

    @Override public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
        return new XtdbFilter(getCluster(), relTraitSet, input, condition);
    }

    @SuppressWarnings("unchecked")
    @Override public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.schema = (Map<Keyword, Object>) filterFn.invoke(implementor.schema, condition);
    }
}
