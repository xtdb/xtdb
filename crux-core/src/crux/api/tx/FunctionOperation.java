package crux.api.tx;

import java.util.Arrays;
import java.util.Objects;

public final class FunctionOperation extends TransactionOperation {
    private final Object id;
    private final Object[] arguments;

    public Object getId() {
        return id;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public static FunctionOperation create(Object id, Object... arguments) {
        return new FunctionOperation(id, arguments);
    }

    private FunctionOperation(Object id, Object[] arguments) {
        this.id = id;
        this.arguments = arguments;
    }

    @Override
    public Type getType() {
        return Type.FN;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
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
