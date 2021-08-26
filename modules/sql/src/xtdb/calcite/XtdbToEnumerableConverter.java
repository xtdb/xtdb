package xtdb.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.DataContext;

// import java.util.AbstractList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in an Elasticsearch data source.
 */
public class XtdbToEnumerableConverter extends ConverterImpl implements EnumerableRel {
    XtdbToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new XtdbToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    @Override public Result implement(EnumerableRelImplementor relImplementor, Prefer pref) {
        final BlockBuilder block = new BlockBuilder();
        final XtdbRel.Implementor implementor = new XtdbRel.Implementor();
        implementor.visitChild(0, getInput());
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of(relImplementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));

        final Expression table = block.append("table", implementor.table .getExpression(XtdbTable.XtdbQueryable.class));

        final Expression schemaExpr = (Expression) XtdbUtils.resolve("xtdb.calcite/->expr").invoke(implementor.schema);
        final Expression schema = block.append("schema", schemaExpr);

        final Expression typesExpr = (Expression) XtdbUtils.resolve("xtdb.calcite/->column-types").invoke(rowType);
        final Expression types = block.append("types", typesExpr);

        Expression enumerable = block.append("enumerable",
                                             Expressions.call(table,
                                                              XtdbMethod.XTDB_QUERYABLE_FIND.method, schema, types, DataContext.ROOT));

        // if (CalciteSystemProperty.DEBUG.value()) {
        //     System.out.println("Mongo: " + opList);
        // }
        // Hook.QUERY_PLAN.run(opList);

        block.add(Expressions.return_(null, enumerable));
        return relImplementor.result(physType, block.toBlock());
    }
}
