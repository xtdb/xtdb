package crux.api;

import java.util.Map;
import java.util.Set;
import clojure.lang.Keyword;

public interface IReadDocumentsAPI {
    /**
     *  Reads a document from the document store based on its
     *  content hash.
     *
     * @param contentHash an object that can be coerced into a content
     * hash.
     * @return            the document map.
     */
    public Map<Keyword, Object> document(Object contentHash);

    /**
     *  Reads a document from the document store based on its
     *  content hash.
     *
     * @param contentHashSet a set of objects that can be coerced into a content
     * hashes.
     * @return            a map from hashable objects to the corresponding documents.
     */
    public Map<String,Map<Keyword,?>> documents(Set<?> contentHashSet);

}
