package xtdb.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import java.util.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.TableScan;

public class XtdbTableScan extends TableScan implements XtdbRel {
    private final XtdbTable xtdbTable;
    private final RelDataType projectRowType;
    private final boolean scanOnly;

    protected XtdbTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                            RelOptTable table, XtdbTable xtdbTable,
                            RelDataType projectRowType,
                            boolean scanOnly) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.xtdbTable = Objects.requireNonNull(xtdbTable, "xtdbTable");
        this.projectRowType = projectRowType;
        this.scanOnly = scanOnly;

        assert getConvention() == XtdbRel.CONVENTION;
    }

    @Override public void register(RelOptPlanner planner) {
        if (scanOnly) {
            for (RelOptRule rule: XtdbRules.SCAN_ONLY_RULES) {
                planner.addRule(rule);
            }
        } else {
            for (RelOptRule rule: XtdbRules.RULES) {
                planner.addRule(rule);
            }
        }
    }

    @Override public void implement(Implementor implementor) {
        implementor.table = table;
        implementor.schema = xtdbTable.schema;
    }

    public XtdbTable getXtdbTable() {
        return xtdbTable;
    }
}
