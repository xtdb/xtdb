package crux.index;

public class BinaryJoinLayeredVirtualIndexPeekState {
    public Object seq;
    public Object key;
    public Object nestedIndexStore;

    public BinaryJoinLayeredVirtualIndexPeekState(Object seq, Object key, Object nestedIndexStore) {
        this.seq = seq;
        this.key = key;
        this.nestedIndexStore = nestedIndexStore;
    }
}
