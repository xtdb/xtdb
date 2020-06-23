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
    private static final Pattern PATTERN  = Pattern.compile("((TRANSACTIONTIME|VALIDTIME)\\s*\\(\\'(.*?)\\'\\))");
    private static final IFn PARSE_DATE_FN = CruxUtils.resolve("clojure.instant/read-instant-date");

    private Date parseDate(String s) {
        return (Date) PARSE_DATE_FN.invoke(s);
    }

    @Override
    public <T> CalciteSignature<T> prepareSql(Context context, Query<T> query, Type elementType, long maxRowCount) {
        if (query.sql != null) {
            Matcher matcher = PATTERN.matcher(query.sql);

            Map<String, Object> internalParameters = new HashMap<String, Object>();
            while (matcher.find()) {
                switch(matcher.group(2))
                {
                    case "TRANSACTIONTIME":
                        Date txTime = parseDate(matcher.group(3));
                        internalParameters.put("TRANSACTIONTIME", txTime);
                        break;
                    case "VALIDTIME":
                        Date validTime = parseDate(matcher.group(3));
                        internalParameters.put("VALIDTIME", validTime);
                        break;
                }
            }

            query = CalcitePrepare.Query.of(matcher.replaceAll("").trim());
            CalciteSignature<T> sig = super.prepareSql(context, query, elementType, maxRowCount);
            sig.internalParameters.putAll(internalParameters);
            return sig;
        }

        return super.prepareSql(context, query, elementType, maxRowCount);
    }
}
