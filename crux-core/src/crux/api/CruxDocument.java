package crux.api;

import clojure.java.api.Clojure;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public final class CruxDocument {
    private static final Keyword DB_ID = Keyword.intern("crux.db/id");
    private static final Keyword FN_ID = Keyword.intern("crux.db/fn");

    private final Object id;
    private final IPersistentMap data;

    private CruxDocument(Object id, IPersistentMap data) {
        this.id = id;
        this.data = data;
    }

    public static CruxDocument buildDoc(Object id, Consumer<Builder> f) {
        Builder builder = new CruxDocument.Builder(id);
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder(Object id) {
        return new Builder(id);
    }

    public static CruxDocument factory(IPersistentMap input) {
        if (input == null) return null;
        Object id = input.valAt(DB_ID);
        if (id == null) throw new RuntimeException(":crux.db/id missing from data map");
        IPersistentMap data = input.without(DB_ID);
        return new CruxDocument(id, data);
    }

    public static class Builder {
        private final Object id;
        private final Map<String, Object> map = new HashMap<>();

        private Builder(Object id) {
            this.id = id;
        }

        public Builder put(String key, Object value) {
            map.put(key, value);
            return this;
        }

        public Builder putAll(Map<String, Object> data) {
            map.putAll(data);
            return this;
        }

        public Builder remove(String key) {
            map.remove(key);
            return this;
        }

        public Builder removeAll(Iterable<String> keys) {
            for (String key: keys) {
                map.remove(key);
            }
            return this;
        }

        public CruxDocument build() {
            IPersistentMap data = PersistentArrayMap.EMPTY;
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                Keyword key = Keyword.intern(entry.getKey());
                assertNotReserved(key);
                data = data.assoc(key, entry.getValue());
            }
            return new CruxDocument(id, data);
        }
    }

    public static CruxDocument create(Object id) {
        return new CruxDocument(id, PersistentArrayMap.EMPTY);
    }

    public static CruxDocument createFunction(Object id, String rawFunction) {
        return new CruxDocument(id, PersistentArrayMap.EMPTY.assoc(FN_ID, Clojure.read(rawFunction)));
    }

    public static CruxDocument create(Object id, Map<String, Object> data) {
        return create(id).putAll(data);
    }

    public CruxDocument put(String key, Object value) {
        Keyword keyword = Keyword.intern(key);
        assertNotReserved(keyword);
        return new CruxDocument(id, data.assoc(keyword, value));
    }

    public CruxDocument putAll(Map<String, Object> entries) {
        IPersistentMap data = this.data;
        for (Map.Entry<String, Object> entry: entries.entrySet()) {
            Keyword key = Keyword.intern(entry.getKey());
            assertNotReserved(key);
            data = data.assoc(key, entry.getValue());
        }
        return new CruxDocument(id, data);
    }

    public CruxDocument remove(String key) {
        return new CruxDocument(id, data.without(Keyword.intern(key)));
    }

    public CruxDocument removeAll(Iterable<String> keys) {
        IPersistentMap data = this.data;
        for (String rawKey: keys) {
            Keyword key = Keyword.intern(rawKey);
            assertNotReserved(key);
            data = data.without(key);
        }
        return new CruxDocument(id, data);
    }

    public Object get(String key) {
        return data.valAt(Keyword.intern(key));
    }

    public Object getId() {
        return id;
    }

    public IPersistentMap toMap() {
        return data.assoc(DB_ID, id);
    }

    private static void assertNotReserved(Keyword key) {
        if (DB_ID.equals(key)) throw new RuntimeException(":crux.db/id is a reserved key");
        if (FN_ID.equals(key)) throw new RuntimeException(":crux.db/fn is a reserved key");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CruxDocument that = (CruxDocument) o;
        return id.equals(that.id) && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data);
    }
}
