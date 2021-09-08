package xtdb.api;

import clojure.java.api.Clojure;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public final class XtdbDocument {
    private static final Keyword DB_ID = Keyword.intern("xt/id");
    private static final Keyword FN_ID = Keyword.intern("xt/fn");

    private final Object id;
    private final IPersistentMap data;

    private XtdbDocument(Object id, IPersistentMap data) {
        this.id = id;
        this.data = data;
    }

    public static XtdbDocument build(Object id, Consumer<Builder> f) {
        Builder builder = builder(id);
        f.accept(builder);
        return builder.build();
    }

    public static Builder builder(Object id) {
        return new Builder(id);
    }

    /**
     * Creates an XtdbDocument from a Clojure map. Not intended for public use, may be removed.
     * @param input
     */
    public static XtdbDocument factory(IPersistentMap input) {
        if (input == null) return null;
        Object id = input.valAt(DB_ID);
        if (id == null) throw new RuntimeException(":xt/id missing from data map");
        IPersistentMap data = input.without(DB_ID);
        return new XtdbDocument(id, data);
    }

    public static class Builder {
        private final Object id;
        private final Map<Keyword, Object> data = new HashMap<>();

        private Builder(Object id) {
            this.id = id;
        }

        @SuppressWarnings("unchecked")
        private Builder(Object id, IPersistentMap data) {
            this.id = id;
            this.data.putAll((Map<Keyword, Object>) data);
        }

        public Builder put(String key, Object value) {
            return put(Keyword.intern(key), value);
        }

        private Builder put(Keyword key, Object value) {
            assertNotReserved(key);
            data.put(key, value);
            return this;
        }

        public Builder putAll(Map<String, Object> data) {
            for (Map.Entry<String, Object> entry: data.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder remove(String key) {
            return remove(Keyword.intern(key));
        }

        private Builder remove(Keyword key) {
            assertNotReserved(key);
            data.remove(key);
            return this;
        }

        public Builder removeAll(Iterable<String> keys) {
            for (String key: keys) {
                remove(key);
            }
            return this;
        }

        public XtdbDocument build() {
            return new XtdbDocument(id, PersistentArrayMap.create(data));
        }
    }

    /**
     * Creates a new {@link XtdbDocument} with just an ID key.
     * See {@link XtdbDocument} for valid ID types
     * @param id
     * @return a new {@link XtdbDocument}
     */
    public static XtdbDocument create(Object id) {
        return new XtdbDocument(id, PersistentArrayMap.EMPTY);
    }

    /**
     * Static factory to create an XTDB transaction function document.
     * @param id the id of the transaction function
     * @param rawFunction the function body, as a Clojure-code string.
     * @return the function document, suitable for use in {@link xtdb.api.tx.Transaction.Builder#put(XtdbDocument)}
     */
    public static XtdbDocument createFunction(Object id, String rawFunction) {
        return new XtdbDocument(id, PersistentArrayMap.EMPTY.assoc(FN_ID, Clojure.read(rawFunction)));
    }

    public static XtdbDocument create(Object id, Map<String, Object> data) {
        return create(id).plusAll(data);
    }

    /**
     * @return a new XtdbDocument with the key/value added
     */
    public XtdbDocument plus(String key, Object value) {
        return toBuilder().put(key, value).build();
    }

    /**
     * @return a new XtdbDocument with the entries added
     */
    public XtdbDocument plusAll(Map<String, Object> entries) {
        return toBuilder().putAll(entries).build();
    }

    /**
     * @return a new XtdbDocument with the key removed
     */
    public XtdbDocument minus(String key) {
        return toBuilder().remove(key).build();
    }

    /**
     * @return a new XtdbDocument with the keys removed
     */
    public XtdbDocument minusAll(Iterable<String> keys) {
        return toBuilder().removeAll(keys).build();
    }

    public Object get(String key) {
        return data.valAt(Keyword.intern(key));
    }

    public Object getId() {
        return id;
    }

    /**
     * Not intended for public use, may be removed.
     *
     * @return this document as a Clojure map.
     */
    public IPersistentMap toMap() {
        return data.assoc(DB_ID, id);
    }

    private static void assertNotReserved(Keyword key) {
        if (DB_ID.equals(key)) throw new IllegalArgumentException(":xt/id is a reserved key");
        if (FN_ID.equals(key)) throw new IllegalArgumentException(":xt/fn is a reserved key");
    }

    private Builder toBuilder() {
        return new Builder(id, data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XtdbDocument that = (XtdbDocument) o;
        return id.equals(that.id) && data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, data);
    }
}
