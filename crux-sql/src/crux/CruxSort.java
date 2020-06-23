package crux.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import clojure.lang.Keyword;
import clojure.lang.IFn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CruxSort extends Sort implements CruxRel {
    private final IFn sortByFn;
    private final IFn limitOffsetFn;

    public CruxSort(RelOptCluster cluster, RelTraitSet traitSet,
                    RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
        assert getConvention() == CruxRel.CONVENTION;
        assert getConvention() == child.getConvention();
        this.sortByFn = CruxUtils.resolveWithErrorLogging("crux.calcite/enrich-sort-by");
        this.limitOffsetFn = CruxUtils.resolveWithErrorLogging("crux.calcite/enrich-limit-and-offset");
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.05);
    }

    @Override public Sort copy(RelTraitSet traitSet, RelNode input,
                               RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new CruxSort(getCluster(), traitSet, input, collation, offset, fetch);
    }

    @SuppressWarnings("unchecked")
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.schema = (Map<Keyword, Object>) sortByFn.invoke(implementor.schema, collation.getFieldCollations());
        implementor.schema = (Map<Keyword, Object>) limitOffsetFn.invoke(implementor.schema, fetch, offset);
    }
}
