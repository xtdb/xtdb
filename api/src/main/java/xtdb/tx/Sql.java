package xtdb.tx;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public final class Sql extends Ops implements Ops.HasArgs<List<?>, Sql> {
    private final String sql;
    private final List<List<?>> argRows;
    private final ByteBuffer argBytes;

    Sql(String sql) {
        this.sql = sql;
        this.argRows = null;
        this.argBytes = null;
    }

    Sql(String sql, ByteBuffer argBytes) {
        this.sql = sql;
        this.argBytes = argBytes;
        this.argRows = null;
    }

    Sql(String sql, List<List<?>> argRows) {
        this.sql = sql;
        this.argRows = argRows;
        this.argBytes = null;
    }

    public String sql() {
        return sql;
    }

    public ByteBuffer argBytes() {
        return argBytes;
    }

    public List<List<?>> argRows() {
        return argRows;
    }

    @Override
    public xtdb.tx.Sql withArgs(List<List<?>> args) {
        return new xtdb.tx.Sql(sql, args);
    }

    @Override
    public String toString() {
        return String.format("[:sql '%s' {:argRows=%s, :argBytes=%s}]", sql, argRows, argBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sql sql1 = (Sql) o;
        return Objects.equals(sql, sql1.sql) && Objects.equals(argRows, sql1.argRows) && Objects.equals(argBytes, sql1.argBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, argRows, argBytes);
    }
}
