package xtdb.calcite;

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;

public class XtdbJdbcDriver extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:xtdb:";

    static {
        new XtdbJdbcDriver().register();
    }

    public XtdbJdbcDriver() {
        super();
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
        return XtdbCalcitePrepareImpl::new;
    }

    @Override protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }
}
