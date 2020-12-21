package crux.api.document;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import crux.api.exception.CruxDocumentException;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public interface ICruxDocument {
    Keyword idKey = Keyword.intern("crux.db/id");

    Object getDocumentId();
    Map<String, Object> getDocumentContents();

    default IPersistentMap toEdn() {
        HashMap<Keyword, Object> document = new HashMap<>();
        Map<String, Object> contents = getDocumentContents();
        Object id = getDocumentId();

        if (!(id instanceof String)
                && !(id instanceof Keyword)
                && !(id instanceof Integer)
                && !(id instanceof Long)
                && !(id instanceof UUID)
                && !(id instanceof URI)
                && !(id instanceof URL)
                && !(id instanceof IPersistentMap)) {
            throw new CruxDocumentException("DocumentId of incorrect type");
        }

        document.put(idKey, id);

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
