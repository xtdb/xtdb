package xtdb.vector;

import clojure.lang.Keyword;
import xtdb.util.NormalForm;

public class KeyFn implements IKeyFn {
    @Override
    public Object denormalize(String k) {
        return k;
    }
    public static KeyFn identity() {
       return new KeyFn();
    }
    public static KeyFn datalog() {
        return new KeyFn() {
            @Override
            public Object denormalize(String k) {
                return Keyword.intern(NormalForm.datalogForm(k));
            }
        };
    }
    public static KeyFn sql() {
        return new KeyFn() {
            @Override
            public Object denormalize(String k) {
                // TODO the inner normalization can probably go once we get rid of things in the RA
                return Keyword.intern(NormalForm.normalForm(k));
            }
        };
    }

    public static KeyFn snakeCase() {

        return new KeyFn() {
            @Override
            public Object denormalize(String k) {
                // TODO the inner normalization can probably go once we get rid of things in the RA
                return Keyword.intern(NormalForm.snakeCase(k));
            }
        };
    }
}