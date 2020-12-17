package crux.api.alphav2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import crux.api.alpha.CruxId;

import java.util.HashMap;
import java.util.Map;

public interface ICruxDocument {
    Keyword idKey = Keyword.intern("crux.db/id");

    CruxId getDocumentId();
    Map<String, Object> getDocumentContents();

    default IPersistentMap generateDocument() throws DocumentIntegrityException {
        HashMap<Keyword, Object> document = new HashMap<>();
        Map<String, Object> contents = getDocumentContents();
        CruxId id = getDocumentId();

        document.put(idKey, id.toEdn());

        for (Map.Entry<String, Object> entry: contents.entrySet()) {
            Keyword key = Keyword.intern(entry.getKey());
            if (key.equals(idKey)) {
                throw new DocumentIntegrityException();
            }
            document.put(key, entry.getValue());
        }

        return PersistentArrayMap.create(document);
    }
}
