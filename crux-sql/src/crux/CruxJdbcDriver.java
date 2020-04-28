package crux.calcite;

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.UnregisteredDriver;
import java.util.Properties;

public class CruxJdbcDriver extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:crux:";

    static {
        new CruxJdbcDriver().register();
    }

    public CruxJdbcDriver() {
        super();
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
        return CruxCalcitePrepareImpl::new;
    }

    @Override protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }
}
