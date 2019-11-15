package crux.api.v2;

import clojure.lang.PersistentVector;

public abstract class Operation {
    Operation() { }

    protected abstract PersistentVector toEdn();
}
