package crux.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;

class CruxRules {
    static final RelOptRule[] RULES = {
        CruxFilterRule.INSTANCE,
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
}
