package crux.calcite;

import org.apache.calcite.linq4j.function.Parameter;
import clojure.lang.Keyword;

public class CruxIdFn {
    public String eval (String x) {
        return ":" + x;
    }
}
