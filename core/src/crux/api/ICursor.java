package crux.api;

import java.util.Iterator;
import java.io.Closeable;

public interface ICursor<E> extends Iterator<E>, Closeable {
}
