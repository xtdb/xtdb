package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Attribute.attr;
import static crux.api.v2.CruxId.*;

public class Document {
    private final Map<Attribute, Object> document;

    private Document(Map<Attribute, Object> attrs) {
        this.document = attrs;
    }

    public Object get(Attribute attr) {
        return document.get(attr);
    }

    public CruxId getId() {
        return cruxId(document.get(Attribute.attr("crux.db/id")));
    }

    public static Document document(CruxId id) {
        Map<Attribute, Object> initialDoc = Collections.singletonMap(Attribute.attr("crux.db/id"), id.toEdn());
        return new Document(initialDoc);
    }

    static Document document(Map<Keyword, ?> map) {
        Map<Attribute, Object> newDoc = new HashMap<>();
        for(Keyword key : map.keySet()) {
            newDoc.put(Attribute.attr(key), map.get(key));
        }
        return new Document(newDoc);
    }

    protected IPersistentMap toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Attribute key : document.keySet()) {
            ednMap = ednMap.assoc(key.toEdn(), document.get(key));
        }
        return ednMap;
    }

    public Document with(Map<Attribute, ?> attrs) {
        Map<Attribute, Object> newDoc = new HashMap<>(this.document);
        newDoc.putAll(attrs);
        return new Document(newDoc);
    }

    public Document with(Attribute attr, Object value) {
        Map<Attribute, Object> newDoc = new HashMap<>(this.document);
        newDoc.put(attr, value);
        return new Document(newDoc);
    }

    public Document with(String strKey, Object value) {
        return with(Attribute.attr(strKey), value);
    }

    @Override
    public String toString() {
        return document.toString();
    }
}
