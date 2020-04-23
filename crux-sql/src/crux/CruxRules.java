package crux.calcite;

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

class CruxRules {
    static final RelOptRule[] RULES = {
        CruxFilterRule.INSTANCE,
        CruxSortRule.INSTANCE,
        CruxProjectRule.INSTANCE,
        CruxJoinRule.INSTANCE,
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

    private static class CruxProjectRule extends ConverterRule {
        private static final CruxProjectRule INSTANCE = new CruxProjectRule();

        private CruxProjectRule() {
            super(LogicalProject.class, Convention.NONE, CruxRel.CONVENTION, "CruxProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(CruxRel.CONVENTION);
            return new CruxProject(project.getCluster(), traitSet, project.getInput(),
                                   project.getProjects(), project.getRowType());
        }
    }

    private static class CruxJoinRule extends ConverterRule {
        private static final CruxJoinRule INSTANCE = new CruxJoinRule();

        private CruxJoinRule() {
            super(LogicalJoin.class, Convention.NONE, CruxRel.CONVENTION, "CruxJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalJoin join = (LogicalJoin) rel;
            final RelTraitSet traitSet = join.getTraitSet().replace(CruxRel.CONVENTION);
            return new CruxJoin(join.getCluster(), traitSet, join.getLeft(), join.getRight(),
                                join.getCondition(), join.getJoinType());
        }
    }
}
