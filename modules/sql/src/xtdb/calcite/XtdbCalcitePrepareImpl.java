package xtdb.calcite;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;

import clojure.lang.IFn;

import java.lang.reflect.Type;
import java.util.Map;

public class XtdbCalcitePrepareImpl extends CalcitePrepareImpl {
    private static final IFn PREPARE_SQL = XtdbUtils.resolve("xtdb.calcite/prepare-sql");

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
