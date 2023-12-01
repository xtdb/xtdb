package xtdb.tx;

import java.nio.ByteBuffer;
import java.util.List;

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
}
