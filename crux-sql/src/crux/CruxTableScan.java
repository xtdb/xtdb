package crux.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptCluster;
import java.util.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.TableScan;

public class CruxTableScan extends TableScan implements CruxRel {
    private final CruxTable cruxTable;
    private final RelDataType projectRowType;

    /**
     * Creates a CruxTableScan.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param table Table
     * @param cruxTable Crux table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected CruxTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                            RelOptTable table, CruxTable cruxTable,
                            RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.cruxTable  = Objects.requireNonNull(cruxTable, "cruxTable");
        this.projectRowType = projectRowType;

        assert getConvention() == CruxRel.CONVENTION;
    }

    @Override public void register(RelOptPlanner planner) {
        planner.addRule(CruxToEnumerableConverterRule.INSTANCE);
    }

    @Override public void implement(Implementor implementor) {
        implementor.cruxTable = cruxTable;
        implementor.table = table;
    }
}
