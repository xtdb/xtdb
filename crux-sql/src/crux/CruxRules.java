package crux.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.core.Sort;

class CruxRules {
    static final RelOptRule[] RULES = {
        CruxFilterRule.INSTANCE,
        CruxLimitRule.INSTANCE,
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

    private static class CruxLimitRule extends RelOptRule {
        private static final CruxLimitRule INSTANCE = new CruxLimitRule();

        private CruxLimitRule() {
            super(operand(EnumerableLimit.class, operand(CruxToEnumerableConverter.class, any())),
                  "CruxLimitRule");
        }

        public RelNode convert(EnumerableLimit limit) {
            final RelTraitSet traitSet = limit.getTraitSet().replace(CruxRel.CONVENTION);
            return new CruxLimit(limit.getCluster(), traitSet,
                                 convert(limit.getInput(), CruxRel.CONVENTION), limit.offset, limit.fetch);
        }

        /** @see org.apache.calcite.rel.convert.ConverterRule */
        public void onMatch(RelOptRuleCall call) {
            final EnumerableLimit limit = call.rel(0);
            final RelNode converted = convert(limit);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }

    private static class CruxSortRule extends CruxConverterRule {
        public static final CruxSortRule INSTANCE = new CruxSortRule();

        private CruxSortRule() {
            super(Sort.class, Convention.NONE, CruxRel.CONVENTION, "CruxSortRule");
        }

        public RelNode convert(RelNode rel) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet =
                sort.getTraitSet().replace(out)
                .replace(sort.getCollation());
            return new CruxSort(rel.getCluster(), traitSet,
                                 convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
                                 sort.getCollation(), sort.offset, sort.fetch);
        }
    }
}
