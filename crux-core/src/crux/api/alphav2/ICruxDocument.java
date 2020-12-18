package crux.api.alphav2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

public interface ICruxDocument {
    Keyword idKey = Keyword.intern("crux.db/id");

    Object getDocumentId();
    Map<String, Object> getDocumentContents();

    default IPersistentMap toEdn() {
        HashMap<Keyword, Object> document = new HashMap<>();
        Map<String, Object> contents = getDocumentContents();
        Object id = getDocumentId();

        document.put(idKey, id);

        for (Map.Entry<String, Object> entry: contents.entrySet()) {
            Keyword key = Keyword.intern(entry.getKey());
            if (key.equals(idKey)) {
                throw new RuntimeException("There is a :crux.db/id key in the document body");
            }
            document.put(key, entry.getValue());
        }

        return PersistentArrayMap.create(document);
    }
}
