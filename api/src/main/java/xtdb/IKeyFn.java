package xtdb;

import clojure.lang.Keyword;
import xtdb.util.NormalForm;

import java.util.HashMap;

public interface IKeyFn<V> {
    V denormalize(String key);

    IKeyFn<String> DATALOG = NormalForm::datalogForm;

    // TODO the inner hyphen to underscore is not strictly necessary on the way out
    IKeyFn<String> SQL = NormalForm::normalForm;

    // TODO the inner hyphen to underscore is not strictly necessary on the way out
    IKeyFn<String> SNAKE_CASE = NormalForm::snakeCase;

    static IKeyFn<Keyword> keyword(IKeyFn<String> f) {
        return (k) -> Keyword.intern(f.denormalize(k));
    }

    static <V> IKeyFn<V> cached(IKeyFn<V> f) {
        var cache = new HashMap<String, V>();
        return (k) -> cache.computeIfAbsent(k, f::denormalize);
    }
}
