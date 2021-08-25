package crux.calcite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Join;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.InvalidRelException;

class CruxRules {
    static final RelOptRule[] RULES = {
        CruxToEnumerableConverterRule.INSTANCE,
        CruxJoinRule.INSTANCE,
        CruxProjectRule.INSTANCE,
        CruxFilterRule.INSTANCE,
        CruxSortRule.INSTANCE,
    };

    static final RelOptRule[] SCAN_ONLY_RULES = {
        CruxToEnumerableConverterRule.INSTANCE,
        CruxFilterRule.INSTANCE,
        CruxSortRule.INSTANCE,
    };


    abstract static class CruxConverterRule extends ConverterRule {
        final Convention out;

        CruxConverterRule(Class<? extends RelNode> clazz, RelTrait in, Convention out,
                          String description) {
            super(clazz, in, out, description);
            this.out = out;
        }
    }

    private static class CruxFilterRule extends CruxConverterRule {
        private static final CruxFilterRule INSTANCE = new CruxFilterRule();

        private CruxFilterRule() {
            super(LogicalFilter.class, Convention.NONE, CruxRel.CONVENTION, "CruxFilterRule");
        }

        @Override public RelNode convert(RelNode relNode) {
            final LogicalFilter filter = (LogicalFilter) relNode;
            final RelTraitSet traitSet = filter.getTraitSet().replace(out);
            return new CruxFilter(relNode.getCluster(), traitSet,
                                  convert(filter.getInput(), out),
                                  filter.getCondition());
        }
    }

    private static class CruxSortRule extends CruxConverterRule {
        public static final CruxSortRule INSTANCE = new CruxSortRule();

        private CruxSortRule() {
            super(Sort.class, Convention.NONE, CruxRel.CONVENTION, "CruxSortRule");
        }

        public RelNode convert(RelNode rel) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
            return new CruxSort(rel.getCluster(), traitSet,
                                convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
                                sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    private static class CruxProjectRule extends CruxConverterRule {
        private static final CruxProjectRule INSTANCE = new CruxProjectRule();

        private CruxProjectRule() {
            super(LogicalProject.class, Convention.NONE, CruxRel.CONVENTION, "CruxProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(CruxRel.CONVENTION);
            return new CruxProject(project.getCluster(), traitSet,
                                   //                                   project.getInput(),
                                   convert(project.getInput(), out),
                                   project.getProjects(), project.getRowType());
        }
    }

    private static class CruxJoinRule extends CruxConverterRule {
        private static final CruxJoinRule INSTANCE = new CruxJoinRule();

        private CruxJoinRule() {
            super(LogicalJoin.class, Convention.NONE, CruxRel.CONVENTION, "CruxJoinRule");
        }

        // Note: this is modeled on org.apache.calcite.adapter.jdbc

        @Override public RelNode convert(RelNode rel) {
            final Join join = (Join) rel;
            switch (join.getJoinType()) {
            case INNER:
                return convert(join, true);
            default:
                // TODO exhaustive study/test of joins available in org.apache.calcite.sql.JoinType
                return null;
            }
        }

        public RelNode convert(Join join, boolean convertInputTraits) {
            final List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : join.getInputs()) {
                if (convertInputTraits && input.getConvention() != getOutTrait()) {
                    input = convert(input, input.getTraitSet().replace(out));
                }
                newInputs.add(input);
            }
            if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
                return null;
            }

            return new CruxJoin(join.getCluster(),
                                join.getTraitSet().replace(out),
                                newInputs.get(0),
                                newInputs.get(1),
                                join.getCondition(),
                                join.getJoinType());
        }

        @SuppressWarnings("fallthrough")
        private boolean canJoinOnCondition(RexNode node) {
            final List<RexNode> operands;
            switch (node.getKind()) {
            case AND:
            case OR:
                operands = ((RexCall) node).getOperands();
                for (RexNode operand : operands) {
                    if (!canJoinOnCondition(operand)) {
                        return false;
                    }
                }
                return true;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                operands = ((RexCall) node).getOperands();
                if ((operands.get(0) instanceof RexInputRef)
                    && (operands.get(1) instanceof RexInputRef)) {
                    return true;
                }
                // fall through

            default:
                return false;
            }
        }
    }
}
