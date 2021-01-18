package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

public class TransactionInstant {
    private static final Keyword TX_ID = Keyword.intern("crux.tx/tx-id");
    private static final Keyword TX_TIME = Keyword.intern("crux.tx/tx-time");

    private final Long id;
    private final Date time;

    public static TransactionInstant factory(Long id, Date time) {
        return new TransactionInstant(id, time);
    }

    public static TransactionInstant factory(Long id) {
        return new TransactionInstant(id, null);
    }

    public static TransactionInstant factory(Map<Keyword, ?> map) {
        if (map == null) {
            return null;
        }

        Long id = (Long) map.get(TX_ID);
        Date time = (Date) map.get(TX_TIME);

        if (id == null && time == null) {
            return null;
        }

        return new TransactionInstant(id, time);
    }

    public static TransactionInstant factory(Date time) {
        return new TransactionInstant(null, time);
    }

    private TransactionInstant(Long id, Date time) {
        this.id = id;
        this.time = time;
    }

    public Long getId() {
        return id;
    }

    public Date getTime() {
        return time;
    }

    public IPersistentMap toMap() {
        IPersistentMap map = PersistentArrayMap.EMPTY;
        if (id != null) {
            map = map.assoc(TX_ID, id);
        }
        if (time != null) {
            map = map.assoc(TX_TIME, time);
        }

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionInstant that = (TransactionInstant) o;
        return Objects.equals(id, that.id) && Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, time);
    }
}