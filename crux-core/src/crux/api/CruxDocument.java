package crux.api;

import clojure.lang.Keyword;

import java.util.HashMap;
import java.util.Map;

public final class CruxDocument extends AbstractCruxDocument {
    private final Object id;
    private final Map<Keyword, Object> data;

    private CruxDocument(Object id, Map<Keyword, Object> data) {
        this.id = id;
        this.data = data;
    }

    public static CruxDocument factory(Map<Keyword, Object> dataRaw) {
        if (dataRaw == null) return null;
        Object id = null;
        Map<Keyword, Object> data = new HashMap<>();
        for (Map.Entry<Keyword, Object> entry: dataRaw.entrySet()) {
            Keyword key = entry.getKey();
            if (DB_ID.equals(key)) {
                id = entry.getValue();
            }
            else {
                data.put(key, entry.getValue());
            }
        }

        if (id == null) throw new RuntimeException(":crux.db/id missing from data map");

        return new CruxDocument(id, data);
    }

    public static CruxDocument create(Object id) {
        return new CruxDocument(id, new HashMap<>());
    }

    public void put(String key, Object value) {
        if (key == null) return;
        put(Keyword.intern(key), value);
    }

    private void put(Keyword key, Object value) {
        if (DB_ID.equals(key)) throw new RuntimeException(":crux.db/id is a reserved key");
        data.put(key, value);
    }

    public Object get(String key) {
        return data.get(Keyword.intern(key));
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    protected Map<Keyword, Object> getData() {
        return data;
    }
}
