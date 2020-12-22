package crux.api.document;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import crux.api.exception.CruxDocumentException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public interface ICruxDocument {
    static boolean equals(ICruxDocument doc, ICruxDocument other) {
        if (doc == null && other == null) return true;
        if (doc == null || other == null) return false;
        return Objects.equals(doc.getDocumentId(), other.getDocumentId())
          && Objects.equals(doc.getDocumentContents(), other.getDocumentContents());
    }

    Keyword idKey = Keyword.intern("crux.db/id");

    Object getDocumentId();
    Map<String, Object> getDocumentContents();

    default IPersistentMap toEdn() {
        HashMap<Keyword, Object> document = new HashMap<>();
        Map<String, Object> contents = getDocumentContents();
        Object id = getDocumentId();
        document.put(idKey, CruxId.validate(id));

        for (Map.Entry<String, Object> entry: contents.entrySet()) {
            Keyword key = Keyword.intern(entry.getKey());
            if (key.equals(idKey)) {
                throw new CruxDocumentException("\"crux.db/id\" is a reserved identifier key");
            }
            document.put(key, entry.getValue());
        }

        return PersistentArrayMap.create(document);
    }
}
