package crux.api;

import java.io.IOException;
import java.util.function.Function;

public class MappedCursor<T, R> implements ICursor<R> {
    private final ICursor<T> origin;
    private final Function<T, R> map;

    public MappedCursor(ICursor<T> origin, Function<T, R> map) {
        this.origin = origin;
        this.map = map;
    }

    @Override
    public void close() throws IOException {
        origin.close();
    }

    @Override
    public boolean hasNext() {
        return origin.hasNext();
    }

    @Override
    public R next() {
        return map.apply(origin.next());
    }
}
