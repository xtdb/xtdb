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

public class CruxCalcitePrepareImpl extends CalcitePrepareImpl {
    private static final Pattern VT_PATTERN  = Pattern.compile("^(VALIDTIME \\(\\'(.*)\\'\\)\\s*)(.*)");

    public CruxCalcitePrepareImpl() {
        super();
    }

    @Override
    public CalciteSignature prepareSql(Context context, Query query, Type elementType, long maxRowCount) {
        if (query.sql != null) {
            Matcher matcher = VT_PATTERN.matcher(query.sql);
            if(matcher.matches()) {
                ZonedDateTime zp = ZonedDateTime.parse(matcher.group(2));
                Date date = Date.from(zp.toInstant());
                query = CalcitePrepare.Query.of(matcher.group(3));
                CalciteSignature sig = super.prepareSql(context, query, elementType, maxRowCount);
                sig.internalParameters.put("VALIDTIME", date);
                return sig;
            }
        }

        return super.prepareSql(context, query, elementType, maxRowCount);
    }
}
