package xtdb.tx;

import clojure.lang.Keyword;
import xtdb.types.ClojureForm;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("StaticInitializerReferencesSubClass")
public class Ops {

    @SuppressWarnings("unused")
    public interface HasArgs<ArgType, O extends HasArgs<ArgType, O>> {
        O withArgs(List<ArgType> args);

        @SuppressWarnings("unchecked")
        default O withArgs(ArgType... args) {
            return withArgs(Arrays.asList(args));
        }
    }

    public interface HasValidTimeBounds<O extends HasValidTimeBounds<O>> {
        O startingFrom(Instant validFrom);

        O until(Instant validTo);

        O during(Instant validFrom, Instant validTo);
    }

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

    public static final class Sql extends Ops implements HasArgs<List<?>, Sql> {
        private final String sql;
        private final List<List<?>> argRows;
        private final ByteBuffer argBytes;

        public Sql(String sql) {
            this.sql = sql;
            this.argRows = null;
            this.argBytes = null;
        }

        private Sql(String sql, ByteBuffer argBytes) {
            this.sql = sql;
            this.argBytes = argBytes;
            this.argRows = null;
        }

        public Sql(String sql, List<List<?>> argRows) {
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
        public String toString() {
            return String.format("[:sql '%s' {:argRows=%s, :argBytes=%s}]", sql, argRows, argBytes);
        }

        @Override
        public Sql withArgs(List<List<?>> args) {
            return new Sql(sql, args);
        }
    }

    public static Xtql xtql(Object query) {
        return new Xtql(query);
    }

    public static final class Xtql extends Ops implements HasArgs<Map<?, ?>, Xtql> {
        private final Object query;
        private final List<Map<?, ?>> args;

        private Xtql(Object query) {
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
        public Xtql withArgs(List<Map<?, ?>> args) {
            return new Xtql(query, args);
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

    public static final class Put extends Ops implements HasValidTimeBounds<Put> {

        private final Keyword tableName;
        private final Map<Keyword, Object> doc;
        private final Instant validFrom;
        private final Instant validTo;

        private Put(Keyword tableName, Map<Keyword, Object> doc) {
            this(tableName, doc, null, null);
        }

        private Put(Keyword tableName, Map<Keyword, Object> doc, Instant validFrom, Instant validTo) {
            this.tableName = tableName;
            this.doc = doc;
            this.validFrom = validFrom;
            this.validTo = validTo;
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

        @Override
        public Put startingFrom(Instant validFrom) {
            return new Put(tableName, doc, validFrom, validTo);
        }

        public Instant validTo() {
            return validTo;
        }

        @Override
        public Put until(Instant validTo) {
            return new Put(tableName, doc, validFrom, validTo);
        }

        @Override
        public Put during(Instant validFrom, Instant validTo) {
            return new Put(tableName, doc, validFrom, validTo);
        }

        @Override
        public String toString() {
            return String.format("[:put {tableName=%s, doc=%s, validFrom=%s, validTo=%s}]", tableName, doc, validFrom, validTo);
        }
    }

    public static Delete delete(Keyword tableName, Object entityId) {
        return new Delete(tableName, entityId);
    }

    public static final class Delete extends Ops implements HasValidTimeBounds<Delete> {

        private final Keyword tableName;
        private final Object entityId;
        private final Instant validFrom;
        private final Instant validTo;

        private Delete(Keyword tableName, Object entityId) {
            this(tableName, entityId, null, null);
        }

        private Delete(Keyword tableName, Object entityId, Instant validFrom, Instant validTo) {
            this.tableName = Objects.requireNonNull(tableName, "expected tableName");
            this.entityId = Objects.requireNonNull(entityId, "expected entityId");
            this.validFrom = validFrom;
            this.validTo = validTo;
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

        @Override
        public Delete startingFrom(Instant validFrom) {
            return new Delete(tableName, entityId, validFrom, validTo);
        }

        public Instant validTo() {
            return validTo;
        }

        @Override
        public Delete until(Instant validTo) {
            return new Delete(tableName, entityId, validFrom, validTo);
        }

        @Override
        public Delete during(Instant validFrom, Instant validTo) {
            return new Delete(tableName, entityId, validFrom, validTo);
        }

        @Override
        public String toString() {
            return String.format("[:delete %s %s {validFrom=%s, validTo=%s}]", tableName, entityId, validFrom, validTo);
        }
    }

    public static Erase erase(Keyword tableName, Object entityId) {
        return new Erase(tableName, entityId);
    }

    public static final class Erase extends Ops {

        private final Keyword tableName;
        private final Object entityId;

        private Erase(Keyword tableName, Object entityId) {
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
            return String.format("[:erase {tableName=%s, entityId=%s}]", tableName, entityId);
        }
    }

    public static Call call(Object fnId, List<?> args) {
        return new Call(fnId, args);
    }

    public static final class Call extends Ops {

        private final Object fnId;
        private final List<?> args;

        public Call(Object fnId, List<?> args) {
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
