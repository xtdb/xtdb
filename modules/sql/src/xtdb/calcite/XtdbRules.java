package xtdb.calcite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.plan.RelOptRule;
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

class XtdbRules {
    static final RelOptRule[] RULES = {
        XtdbToEnumerableConverterRule.INSTANCE,
        XtdbJoinRule.INSTANCE,
        XtdbProjectRule.INSTANCE,
        XtdbFilterRule.INSTANCE,
        XtdbSortRule.INSTANCE,
    };

    static final RelOptRule[] SCAN_ONLY_RULES = {
        XtdbToEnumerableConverterRule.INSTANCE,
        XtdbFilterRule.INSTANCE,
        XtdbSortRule.INSTANCE,
    };


    abstract static class XtdbConverterRule extends ConverterRule {
        final Convention out;

        XtdbConverterRule(Class<? extends RelNode> clazz, RelTrait in, Convention out,
                          String description) {
            super(clazz, in, out, description);
            this.out = out;
        }
    }

    private static class XtdbFilterRule extends XtdbConverterRule {
        private static final XtdbFilterRule INSTANCE = new XtdbFilterRule();

        private XtdbFilterRule() {
            super(LogicalFilter.class, Convention.NONE, XtdbRel.CONVENTION, "XtdbFilterRule");
        }

        @Override public RelNode convert(RelNode relNode) {
            final LogicalFilter filter = (LogicalFilter) relNode;
            final RelTraitSet traitSet = filter.getTraitSet().replace(out);
            return new XtdbFilter(relNode.getCluster(), traitSet,
                                  convert(filter.getInput(), out),
                                  filter.getCondition());
        }
    }

    private static class XtdbSortRule extends XtdbConverterRule {
        public static final XtdbSortRule INSTANCE = new XtdbSortRule();

        private XtdbSortRule() {
            super(Sort.class, Convention.NONE, XtdbRel.CONVENTION, "XtdbSortRule");
        }

        public RelNode convert(RelNode rel) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
            return new XtdbSort(rel.getCluster(), traitSet,
                                convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
                                sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    private static class XtdbProjectRule extends XtdbConverterRule {
        private static final XtdbProjectRule INSTANCE = new XtdbProjectRule();

        private XtdbProjectRule() {
            super(LogicalProject.class, Convention.NONE, XtdbRel.CONVENTION, "XtdbProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(XtdbRel.CONVENTION);
            return new XtdbProject(project.getCluster(), traitSet,
                                   //                                   project.getInput(),
                                   convert(project.getInput(), out),
                                   project.getProjects(), project.getRowType());
        }
    }

    private static class XtdbJoinRule extends XtdbConverterRule {
        private static final XtdbJoinRule INSTANCE = new XtdbJoinRule();

        private XtdbJoinRule() {
            super(LogicalJoin.class, Convention.NONE, XtdbRel.CONVENTION, "XtdbJoinRule");
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

            return new XtdbJoin(join.getCluster(),
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
