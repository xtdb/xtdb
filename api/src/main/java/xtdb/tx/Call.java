package xtdb.tx;

import java.util.List;
import java.util.Objects;

public final class Call extends Ops {

    private final Object fnId;
    private final List<?> args;

    Call(Object fnId, List<?> args) {
        this.fnId = fnId;
        this.args = args;
    }

    public Object fnId() {
        return fnId;
    }

    public List<?> args() {
        return args;
    }

    @Override
    public String toString() {
        return String.format("[:call %s %s]", fnId, args);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Call call = (Call) o;
        return Objects.equals(fnId, call.fnId) && Objects.equals(args, call.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fnId, args);
    }
}
