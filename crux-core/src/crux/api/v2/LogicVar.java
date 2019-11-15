package crux.api.v2;

import clojure.lang.Symbol;

import java.util.Objects;

public class LogicVar {
    private final Symbol sym;

    private LogicVar(Symbol sym) {
        this.sym = sym;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicVar logicVar = (LogicVar) o;
        return sym.equals(logicVar.sym);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sym);
    }

    public static LogicVar logicVar(String name) {
        return new LogicVar(Symbol.intern(name));
    }

    public static LogicVar logicVar(Symbol symbol) {
        return new LogicVar(symbol);
    }

    protected Symbol toEdn() { return sym; }
}

