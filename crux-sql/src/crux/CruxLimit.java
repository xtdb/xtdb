package crux.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.util.List;
import java.util.Map;

public class CruxLimit extends SingleRel implements CruxRel {
    private final IFn limitOffsetFn;
    public final RexNode offset;
    public final RexNode fetch;

    public CruxLimit(RelOptCluster cluster, RelTraitSet traitSet,
                     RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input);
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() == input.getConvention();
        this.limitOffsetFn = CruxUtils.resolve("crux.calcite/enrich-limit-and-offset");
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // We do this so we get the limit for free
        return planner.getCostFactory().makeZeroCost();
    }

    @Override public CruxLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
        return new CruxLimit(getCluster(), traitSet, sole(newInputs), offset, fetch);
    }

    @SuppressWarnings("unchecked")
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.schema = (Map<Keyword, Object>) limitOffsetFn.invoke(implementor.schema, fetch, offset);
    }

    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }
}
