package crux.api.document;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.MapEntry;

import java.util.HashMap;
import java.util.Map;

public class CruxDocument implements ICruxDocument {
    private final Object id;
    private final Map<String, Object> data;

    public CruxDocument(Object id, Map<String, Object> data) {
        this.id = id;
        this.data = data;
    }

    public static CruxDocument factory(IPersistentMap input) {
        Object id = input.valAt(ICruxDocument.idKey);
        HashMap<String, Object> data = new HashMap<>();
        for (Object o: input) {
            MapEntry entry = (MapEntry) o;
            Keyword key = (Keyword) entry.getKey();

            if (key.equals(ICruxDocument.idKey)) {
                continue;
            }

            Object value = entry.getValue();
            String string = key.getNamespace() + "/" + key.getName();
            data.put(string, value);
        }
        return new CruxDocument(id, data);
    }

    public static CruxDocument factory(Map<Keyword, Object> input) {
        Object id = input.get(ICruxDocument.idKey);
        HashMap<String, Object> data = new HashMap<>();
        for (Map.Entry<Keyword, Object> entry: input.entrySet()) {
            Keyword key = entry.getKey();

            if (key.equals(ICruxDocument.idKey)) {
                continue;
            }

            Object value = entry.getValue();
            String string = key.getNamespace() + "/" + key.getName();
            data.put(string, value);
        }
        return new CruxDocument(id, data);
    }

    public Object get(String key) {
        return data.get(key);
    }

    @Override
    public Object getDocumentId() {
        return id;
    }

    @Override
    public Map<String, Object> getDocumentContents() {
        return data;
    }
}
