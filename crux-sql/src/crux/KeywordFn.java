package crux.calcite;

import org.apache.calcite.linq4j.function.Parameter;
import clojure.lang.Keyword;

public class KeywordFn {
    public Keyword eval (String x) {
        return Keyword.intern(x);
    }
}
