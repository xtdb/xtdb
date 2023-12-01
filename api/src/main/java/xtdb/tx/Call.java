package xtdb.tx;

import java.util.List;

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
}
