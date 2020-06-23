package crux.calcite;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.InvalidRelException;

public class CruxJoin extends Join implements CruxRel {
    private RelOptTable table;
    private final IFn joinFn;

    public CruxJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                    RexNode condition, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, ImmutableSet.of(), joinType);
        this.joinFn = CruxUtils.resolveWithErrorLogging("crux.calcite/enrich-join");
        assert getConvention() == CruxRel.CONVENTION;
    }

    @Override public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
                               RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new CruxJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @SuppressWarnings("unchecked")
    @Override public void implement(Implementor implementor) {
        implementor.visitChild(0, getLeft());
        Map<Keyword, Object> schema1 = implementor.schema;
        implementor.visitChild(0, getRight());
        Map<Keyword, Object> schema2 = implementor.schema;
        implementor.schema = (Map<Keyword, Object>) joinFn.invoke(schema1, schema2, getJoinType(), getCondition());
    }

    @Override public RelOptTable getTable() {
        return getLeft().getTable();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }
}
