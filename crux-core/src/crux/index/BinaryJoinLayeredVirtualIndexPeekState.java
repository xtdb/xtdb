package crux.index;

public class BinaryJoinLayeredVirtualIndexPeekState {
    public Object seq;
    public Object key;

    public BinaryJoinLayeredVirtualIndexPeekState(Object seq, Object key) {
        this.seq = seq;
        this.key = key;
    }
}
