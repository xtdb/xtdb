package crux.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
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

public class CruxJoin extends Join implements CruxRel {
    private RelOptTable table;
    private final IFn joinFn;

    public CruxJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                    RexNode condition, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, ImmutableSet.of(), joinType);
        this.joinFn = CruxUtils.resolve("crux.calcite/enrich-join");
        assert getConvention() == CruxRel.CONVENTION;
    }

    @Override public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
                               RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new CruxJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @SuppressWarnings("unchecked")
    @Override public void implement(Implementor implementor) {
        implementor.visitChild(0, getLeft());
        implementor.schema = (Map<Keyword, Object>) joinFn.invoke(getLeft(), getRight(), getJoinType(), getCondition());
    }

    @Override public RelOptTable getTable() {
        return getLeft().getTable();
    }
}
