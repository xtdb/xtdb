package crux.api.alphav2.transaction.operation;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.List;

public abstract class TransactionOperation {
    enum Type {
        PUT("crux.tx/put"),
        MATCH("crux.tx/match)"),
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
        return switch (type) {
            case PUT -> PutTransactionOperation.factory(vector);
            case MATCH -> MatchTransactionOperation.factory(vector);
            case DELETE -> DeleteTransactionOperation.factory(vector);
            case EVICT -> EvictTransactionOperation.factory(vector);
        };
    }

    private final Type type;

    TransactionOperation(Type type) {
        this.type = type;
    }

    abstract List<Object> getArgs();

    public PersistentVector toEdn() {
        PersistentVector ret = PersistentVector.create(type);
        ret.addAll(getArgs());
        return ret;
    }
}
