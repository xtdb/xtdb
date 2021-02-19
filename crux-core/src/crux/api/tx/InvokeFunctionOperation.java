package crux.api.tx;

import java.util.Arrays;
import java.util.Objects;

public final class InvokeFunctionOperation extends TransactionOperation {
    private final Object id;
    private final Object[] arguments;

    public Object getId() {
        return id;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public static InvokeFunctionOperation create(Object id, Object... arguments) {
        return new InvokeFunctionOperation(id, arguments);
    }

    private InvokeFunctionOperation(Object id, Object[] arguments) {
        this.id = id;
        this.arguments = arguments;
    }

    @Override
    public <E> E accept(Visitor<E> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvokeFunctionOperation that = (InvokeFunctionOperation) o;
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
