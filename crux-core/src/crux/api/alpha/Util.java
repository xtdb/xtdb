package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.Symbol;

public class Util {
    private Util() {}

    public static Keyword keyword(String s) {
        return Keyword.intern(s);
    }

    public static Symbol symbol(String s) {
        return Symbol.intern(s);
    }
}
