package xtdb.tx;

import clojure.lang.Keyword;
import xtdb.types.ClojureForm;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    Ops() {
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

    public static Xtql xtql(Object query) {
        return new Xtql(query);
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

    public static Delete delete(Keyword tableName, Object entityId) {
        return new Delete(tableName, entityId);
    }

    public static Erase erase(Keyword tableName, Object entityId) {
        return new Erase(tableName, entityId);
    }

    public static Call call(Object fnId, List<?> args) {
        return new Call(fnId, args);
    }

    public static final Abort ABORT = new Abort();

}
