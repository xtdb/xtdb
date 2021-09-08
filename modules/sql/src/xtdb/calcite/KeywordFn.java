package xtdb.calcite;

import clojure.lang.Keyword;

public class KeywordFn {
    public Keyword eval (String x) {
        return Keyword.intern(x);
    }
}
