package xtdb.vector;

import clojure.lang.Keyword;
import xtdb.util.NormalForm;

public class KeyFnBuilder {
    public static IKeyFn datalog() {
        return (k) -> Keyword.intern(NormalForm.datalogForm(k));
    }

    public static IKeyFn sql() {
        // TODO the inner normalisation is not strictly necessary on the way out
        return (k) -> Keyword.intern(NormalForm.normalForm(k));
    }

    public static IKeyFn snakeCase() {
        // TODO the inner hyphen to underscore is not strictly necessary on the way out
        return (k) -> Keyword.intern(NormalForm.snakeCase(k));
    }
}