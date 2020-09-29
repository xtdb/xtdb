package crux.index;

public class IndexStoreIndexState {
    public Object iterator;
    public Object key;

    public IndexStoreIndexState(Object iterator, Object key) {
        this.iterator = iterator;
        this.key = key;
    }
}
