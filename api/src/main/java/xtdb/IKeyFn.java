package xtdb;

import clojure.lang.Keyword;
import xtdb.util.NormalForm;

import java.util.HashMap;

public interface IKeyFn {
    Object denormalize(String key);

    IKeyFn DATALOG = (k) -> Keyword.intern(NormalForm.datalogForm(k));

    // TODO the inner normalisation is not strictly necessary on the way out
    IKeyFn SQL = (k) -> Keyword.intern(NormalForm.normalForm(k));

    // TODO the inner hyphen to underscore is not strictly necessary on the way out
    IKeyFn SNAKE_CASE = (k) -> Keyword.intern(NormalForm.snakeCase(k));

    static IKeyFn cached(IKeyFn f) {
        var cache = new HashMap<String, Object>();
        return (k) -> cache.computeIfAbsent(k, f::denormalize);
    }
}
