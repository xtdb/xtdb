package crux.api;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import clojure.lang.Keyword;

public interface TxLogIterator extends Iterator<Map<Keyword, ?>>, Closeable {
}
