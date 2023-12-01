package xtdb.tx;

public final class Abort extends Ops {
    Abort() {
    }

    @Override
    public String toString() {
        return "[:abort]";
    }
}
