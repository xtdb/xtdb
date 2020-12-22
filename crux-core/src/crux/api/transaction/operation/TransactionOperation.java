package crux.api.transaction.operation;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.List;
import java.util.Objects;

public abstract class TransactionOperation {
    enum Type {
        PUT("crux.tx/put"),
        MATCH("crux.tx/match"),
        DELETE("crux.tx/delete"),
        EVICT("crux.tx/evict");

        private final Keyword keyword;

        Type(String key) {
            keyword = Keyword.intern(key);
        }

        private static Type valueOf(Keyword keyword) {
            for (Type type: values()) {
                if (type.keyword.equals(keyword)) {
                    return type;
                }
            }
            return null;
        }
    }

    public static TransactionOperation factory(PersistentVector vector) {
        Keyword typeKeyword = (Keyword) vector.get(0);
        Type type = Type.valueOf(typeKeyword);
        assert type != null;
        switch (type) {
            case PUT: return PutTransactionOperation.factory(vector);
            case MATCH: return MatchTransactionOperation.factory(vector);
            case DELETE: return DeleteTransactionOperation.factory(vector);
            case EVICT: return EvictTransactionOperation.factory(vector);
        }
        return null;
    }

    protected final Type type;

    TransactionOperation(Type type) {
        this.type = type;
    }

    abstract List<Object> getArgs();

    public PersistentVector toEdn() {
        PersistentVector ret = PersistentVector.create(type.keyword);
        List<Object> args = getArgs();
        for (Object arg: args) {
            ret = ret.cons(arg);
        }
        return ret;
    }
}
