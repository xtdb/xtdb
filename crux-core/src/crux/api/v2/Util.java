package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.Symbol;

public class Util {
    private Util() {}

    public static Keyword kw(String s) {
        return Keyword.intern(s);
    }

    public static Symbol sym(String s) {
        return Symbol.intern(s);
    }
}
