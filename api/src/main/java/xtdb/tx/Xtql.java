package xtdb.tx;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Xtql extends Ops implements Ops.HasArgs<Map<?, ?>, Xtql> {
    private final Object query;
    private final List<Map<?, ?>> args;

    Xtql(Object query) {
        this(query, null);
    }

    private Xtql(Object query, List<Map<?, ?>> args) {
        this.query = query;
        this.args = args;
    }

    public Object query() {
        return query;
    }

    public List<?> args() {
        return args;
    }

    @Override
    public String toString() {
        return String.format("[:xtql %s {:args %s}]", query, args);
    }

    @Override
    public xtdb.tx.Xtql withArgs(List<Map<?, ?>> args) {
        return new xtdb.tx.Xtql(query, args);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Xtql xtql = (Xtql) o;
        return Objects.equals(query, xtql.query) && Objects.equals(args, xtql.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, args);
    }
}
