package crux.calcite;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;

import clojure.lang.IFn;

import java.lang.reflect.Type;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

public class CruxCalcitePrepareImpl extends CalcitePrepareImpl {
    private static final IFn PREPARE_SQL = CruxUtils.resolve("crux.calcite/prepare-sql");

    public interface PreparedSQL {
        String query();
        Map<String, ?> internalParameters();
    }

    @Override
    public <T> CalciteSignature<T> prepareSql(Context context, Query<T> query, Type elementType, long maxRowCount) {
        if (query.sql == null) return super.prepareSql(context, query, elementType, maxRowCount);

        PreparedSQL preparedSQL = (PreparedSQL) PREPARE_SQL.invoke(query.sql);

        query = CalcitePrepare.Query.of(preparedSQL.query());

        CalciteSignature<T> sig = super.prepareSql(context, query, elementType, maxRowCount);
        sig.internalParameters.putAll(preparedSQL.internalParameters());
        return sig;
    }
}
