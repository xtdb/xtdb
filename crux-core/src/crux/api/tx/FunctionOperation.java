package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;

import java.util.Arrays;
import java.util.Objects;

public final class FunctionOperation extends TransactionOperation {
    private final Object id;
    private final Object[] arguments;

    public static FunctionOperation create(Object id, Object... arguments) {
        return new FunctionOperation(id, arguments);
    }

    private FunctionOperation(Object id, Object[] arguments) {
        this.id = id;
        this.arguments = arguments;
    }

    @Override
    public IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.FN.getKeyword())
                .cons(id);
        for (Object argument: arguments) {
            ret = ret.cons(argument);
        }
        return ret;
    }

    @Override
    public Type getType() {
        return Type.FN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionOperation that = (FunctionOperation) o;
        return id.equals(that.id)
                && Arrays.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id);
        result = 31 * result + Arrays.hashCode(arguments);
        return result;
    }
}
