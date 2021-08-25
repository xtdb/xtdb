package crux.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class CruxToEnumerableConverterRule extends ConverterRule {
    static final ConverterRule INSTANCE =
        new CruxToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

    private CruxToEnumerableConverterRule(RelBuilderFactory relBuilderFactory) {
        super(RelNode.class, (Predicate<RelNode>) r -> true,
              CruxRel.CONVENTION, EnumerableConvention.INSTANCE,
              relBuilderFactory, "CruxToEnumerableConverterRule");
    }

    @Override public RelNode convert(RelNode relNode) {
        RelTraitSet newTraitSet = relNode.getTraitSet().replace(getOutConvention());
        return new CruxToEnumerableConverter(relNode.getCluster(), newTraitSet, relNode);
    }
}
