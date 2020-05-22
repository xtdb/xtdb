package crux.calcite;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;

import java.lang.reflect.Type;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

public class CruxCalcitePrepareImpl extends CalcitePrepareImpl {
    private static final Pattern PATTERN  = Pattern.compile("((TRANSACTIONTIME|VALIDTIME|TRANSACTIONID)\\s\\(\\'(.*?)\\'\\))");

    public CruxCalcitePrepareImpl() {
        super();
    }

    @Override
    public <T> CalciteSignature<T> prepareSql(Context context, Query<T> query, Type elementType, long maxRowCount) {
        if (query.sql != null) {
            Matcher matcher = PATTERN.matcher(query.sql);

            Map<String, Object> internalParameters = new HashMap<String, Object>();
            while (matcher.find()) {
                switch(matcher.group(2))
                {
                    case "TRANSACTIONID":
                        internalParameters.put("TRANSACTIONID", Long.parseLong(matcher.group(3)));
                        break;
                    case "TRANSACTIONTIME":
                        Date txTime = Date.from(ZonedDateTime.parse(matcher.group(3)).toInstant());
                        internalParameters.put("TRANSACTIONTIME", txTime);
                        break;
                    case "VALIDTIME":
                        Date validTime = Date.from(ZonedDateTime.parse(matcher.group(3)).toInstant());
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
