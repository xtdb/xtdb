package crux.api;

import clojure.lang.ILookup;
import clojure.lang.Keyword;

import java.util.Map;

public interface IDocument extends ILookup {
    Object getDocumentId();

    Map<Keyword, ?> getContents();
}
