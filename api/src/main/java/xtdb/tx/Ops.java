package xtdb.tx;

import clojure.lang.Keyword;
import xtdb.types.ClojureForm;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SuppressWarnings("StaticInitializerReferencesSubClass")
public sealed class Ops {
    private Ops() {
    }

    public static Sql sql(String sql) {
        return new Sql(sql);
    }

    public static Sql sql(String sql, List<?> paramRow) {
        return new Sql(sql, List.of(paramRow));
    }

    public static Sql sqlBatch(String sql, List<List<?>> paramGroupRows) {
        return new Sql(sql, paramGroupRows);
    }

    public static Sql sqlBatch(String sql, ByteBuffer paramGroupBytes) {
        return new Sql(sql, paramGroupBytes);
    }

    public static Sql sqlBatch(String sql, byte[] paramGroupBytes) {
        return sqlBatch(sql, ByteBuffer.wrap(paramGroupBytes));
    }

    public static final class Sql extends Ops {
        private final String sql;
        private final List<List<?>> paramGroupRows;
        private final ByteBuffer paramGroupBytes;

        public Sql(String sql) {
            this.sql = sql;
            this.paramGroupRows = null;
            this.paramGroupBytes = null;
        }

        private Sql(String sql, ByteBuffer paramGroupBytes) {
            this.sql = sql;
            this.paramGroupBytes = paramGroupBytes;
            this.paramGroupRows = null;
        }

        public Sql(String sql, List<List<?>> paramGroupRows) {
            this.sql = sql;
            this.paramGroupRows = paramGroupRows;
            this.paramGroupBytes = null;
        }

        public String sql() {
            return sql;
        }

        public ByteBuffer paramGroupBytes() {
            return paramGroupBytes;
        }

        public List<List<?>> paramGroupRows() {
            return paramGroupRows;
        }

        @Override
        public String toString() {
            return "[:sql '%s' {:paramGroupRows=%s, :paramGroupBytes=%s}]".formatted(sql, paramGroupRows, paramGroupBytes);
        }
    }

    public static Put put(Keyword tableName, Map<Keyword, Object> doc) {
        return new Put(tableName, doc);
    }

    private static final Keyword XT_TXS = Keyword.intern("xt", "tx-fns");
    private static final Keyword XT_ID = Keyword.intern("xt", "id");
    private static final Keyword XT_FN = Keyword.intern("xt", "fn");

    public static Put putFn(Object fnId, Object fnForm) {
        return put(XT_TXS, Map.of(XT_ID, fnId, XT_FN, new ClojureForm(fnForm)));
    }

    public static final class Put extends Ops {

        private final Keyword tableName;
        private final Map<Keyword, Object> doc;
        private Instant validFrom;
        private Instant validTo;

        private Put(Keyword tableName, Map<Keyword, Object> doc) {
            this.tableName = tableName;
            this.doc = doc;
        }

        public Keyword tableName() {
            return tableName;
        }

        public Map<Keyword, Object> doc() {
            return doc;
        }

        public Instant validFrom() {
            return validFrom;
        }

        public Put validFrom(Instant validFrom) {
            this.validFrom = validFrom;
            return this;
        }

        public Put validFrom(Date validFrom) {
            this.validFrom = validFrom.toInstant();
            return this;
        }

        public Instant validTo() {
            return validTo;
        }

        public Put validTo(Instant validTo) {
            this.validTo = validTo;
            return this;
        }

        public Put validTo(Date validTo) {
            this.validTo = validTo.toInstant();
            return this;
        }

        @Override
        public String toString() {
            return "[:put {tableName=%s, doc=%s, validFrom=%s, validTo=%s}]".formatted(tableName, doc, validFrom, validTo);
        }
    }

    public static Delete delete(Keyword tableName, Object entityId) {
        return new Delete(tableName, entityId);
    }

    public static final class Delete extends Ops {

        private final Keyword tableName;
        private final Object entityId;
        private Instant validFrom;
        private Instant validTo;

        private Delete(Keyword tableName, Object entityId) {
            this.tableName = tableName;
            this.entityId = entityId;
        }

        public Keyword tableName() {
            return tableName;
        }

        public Object entityId() {
            return entityId;
        }

        public Instant validFrom() {
            return validFrom;
        }

        public Delete validFrom(Instant validFrom) {
            this.validFrom = validFrom;
            return this;
        }

        public Delete validFrom(Date validFrom) {
            this.validFrom = validFrom.toInstant();
            return this;
        }

        public Instant validTo() {
            return validTo;
        }

        public Delete validTo(Instant validTo) {
            this.validTo = validTo;
            return this;
        }

        public Delete validTo(Date validTo) {
            this.validTo = validTo.toInstant();
            return this;
        }

        @Override
        public String toString() {
            return "[:delete %s %s {validFrom=%s, validTo=%s}]".formatted(tableName, entityId, validFrom, validTo);
        }
    }

    public static Evict evict(Keyword tableName, Object entityId) {
        return new Evict(tableName, entityId);
    }

    public static final class Evict extends Ops {

        private final Keyword tableName;
        private final Object entityId;

        private Evict(Keyword tableName, Object entityId) {
            this.tableName = tableName;
            this.entityId = entityId;
        }

        public Keyword tableName() {
            return tableName;
        }

        public Object entityId() {
            return entityId;
        }

        @Override
        public String toString() {
            return "[:evict {tableName=%s, entityId=%s}]".formatted(tableName, entityId);
        }
    }

    public static Call call(Object fnId, Object... args) {
        return new Call(fnId, args);
    }

    public static final class Call extends Ops {

        private final Object fnId;
        private final Object[] args;

        public Call(Object fnId, Object[] args) {
            this.fnId = fnId;
            this.args = args;
        }

        public Object fnId() {
            return fnId;
        }

        public Object[] args() {
            return args;
        }

        @Override
        public String toString() {
            return "[:call %s %s]".formatted(fnId, Arrays.toString(args));
        }
    }

    public static final Abort ABORT = new Abort();

    public static final class Abort extends Ops {
        private Abort() {
        }

        @Override
        public String toString() {
            return "[:abort]";
        }
    }
}
