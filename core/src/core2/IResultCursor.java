package core2;

import java.util.Map;

public interface IResultCursor<E> extends ICursor<E> {
    Map<String, Object> columnTypes();
}
