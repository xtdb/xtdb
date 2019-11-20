package crux.api.alpha;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static crux.api.alpha.CruxId.cruxId;
import static crux.api.alpha.Util.keyword;

public class Document {
    private final Map<Keyword, Object> document;

    private Document(Map<Keyword, Object> attrs) {
        this.document = attrs;
    }

    /**
     * Gets a value at the key 'attr' from the Document
     * @param attr The key to extract a value from
     * @return Value at key
     */
    public Object get(Keyword attr) {
        return document.get(attr);
    }

    /**
     * Gets a value at the key 'attr' from the Document
     * @param attr The key to extract a value from
     * @return Value at key
     */
    public Object get(String attr) {
        return document.get(keyword(attr));
    }

    /**
     * Returns the CruxId from the Document.
     * @return CruxId from the Document.
     */
    public CruxId getId() {
        return cruxId(document.get(keyword("crux.db/id")));
    }

    /**
     * Construct a Document containing a CruxId
     * @param id Id to put in the document
     * @return An instance of Document
     */
    public static Document document(CruxId id) {
        Map<Keyword, Object> initialDoc = Collections.singletonMap(keyword("crux.db/id"), id.toEdn());
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

    /**
     * Put the contents of a map into the Document
     * @param attrs Map of keywords and values to add to the document
     * @return Document containing the previous contents of the document and the attributes
     */
    public Document with(Map<Keyword, ?> attrs) {
        Map<Keyword, Object> newDoc = new HashMap<>(this.document);
        newDoc.putAll(attrs);
        return new Document(newDoc);
    }

    /**
     * Places the key-value pair (attr, value) into the Document
     * @param attr Key to place the value at
     * @param value Value to place in the Document
     * @return Document containing the previous contents of the document and the new key-value pair
     */
    public Document with(Keyword attr, Object value) {
        Map<Keyword, Object> newDoc = new HashMap<>(this.document);
        newDoc.put(attr, value);
        return new Document(newDoc);
    }

    /**
     * Places the key-value pair (attr, value) into the Document
     * @param attr Key to place the value at
     * @param value Value to place in the Document
     * @return Document containing the previous contents of the document and the new key-value pair
     */
    public Document with(String attr, Object value) {
        return with(keyword(attr), value);
    }

    @Override
    public String toString() {
        return document.toString();
    }
}
