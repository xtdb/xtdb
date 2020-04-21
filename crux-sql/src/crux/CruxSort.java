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

import java.util.ArrayList;
import java.util.List;

public class CruxSort extends Sort implements CruxRel {
  public CruxSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, collation, offset, fetch);
    assert getConvention() == CruxRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.05);
  }

  @Override public Sort copy(RelTraitSet traitSet, RelNode input,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new CruxSort(getCluster(), traitSet, input, collation, offset,
        fetch);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (!collation.getFieldCollations().isEmpty()) {
      final List<String> keys = new ArrayList<>();
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        implementor.sortField = fieldCollation.getFieldIndex();
        implementor.sortDirection = direction(fieldCollation);
      }
    }
  }

  private int direction(RelFieldCollation fieldCollation) {
    switch (fieldCollation.getDirection()) {
    case DESCENDING:
    case STRICTLY_DESCENDING:
      return -1;
    case ASCENDING:
    case STRICTLY_ASCENDING:
    default:
      return 1;
    }
  }
}
