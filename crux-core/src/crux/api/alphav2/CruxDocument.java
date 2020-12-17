package crux.api.alphav2;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import crux.api.alpha.CruxId;

import java.util.HashMap;
import java.util.Map;

public class CruxDocument implements ICruxDocument {
    private final CruxId id;
    private final Map<String, Object> data;

    public CruxDocument(PersistentArrayMap input) {
        id = new CruxId(input.valAt(ICruxDocument.idKey));
        data = new HashMap<>();
        for (Object keyRaw: input) {
            Keyword key = (Keyword) keyRaw;
            if (key.equals(ICruxDocument.idKey)) {
                continue;
            }

            String string = key.toString();
            data.put(string, input.get(keyRaw));
        }
    }

    public Object get(String key) {
        return data.get(key);
    }

    @Override
    public CruxId getDocumentId() {
        return id;
    }

    @Override
    public Map<String, Object> getDocumentContents() {
        return data;
    }
}
