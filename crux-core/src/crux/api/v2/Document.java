package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.CruxId.cruxId;

public class Document {
    private final Map<Keyword, Object> document;

    private Document(Map<Keyword, Object> attrs) {
        this.document = attrs;
    }

    public Object get(Keyword attr) {
        return document.get(attr);
    }

    public Object get(String attr) {
        return document.get(Util.keyword(attr));
    }

    public CruxId getId() {
        return cruxId(document.get(Util.keyword("crux.db/id")));
    }

    public static Document document(CruxId id) {
        Map<Keyword, Object> initialDoc = Collections.singletonMap(Util.keyword("crux.db/id"), id.toEdn());
        return new Document(initialDoc);
    }

    static Document document(Map<Keyword, Object> document) {
        return new Document(document);
    }

    protected IPersistentMap toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Keyword key : document.keySet()) {
            ednMap = ednMap.assoc(key, document.get(key));
        }
        return ednMap;
    }

    public Document with(Map<Keyword, ?> attrs) {
        Map<Keyword, Object> newDoc = new HashMap<>(this.document);
        newDoc.putAll(attrs);
        return new Document(newDoc);
    }

    public Document with(Keyword attr, Object value) {
        Map<Keyword, Object> newDoc = new HashMap<>(this.document);
        newDoc.put(attr, value);
        return new Document(newDoc);
    }

    public Document with(String strKey, Object value) {
        return with(Util.keyword(strKey), value);
    }

    @Override
    public String toString() {
        return document.toString();
    }
}
