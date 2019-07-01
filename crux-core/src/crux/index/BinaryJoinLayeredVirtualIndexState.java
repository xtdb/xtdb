package crux.index;

public class BinaryJoinLayeredVirtualIndexState {
    public Object indexes;
    public long depth;

    public BinaryJoinLayeredVirtualIndexState(Object indexes, long depth) {
        this.indexes = indexes;
        this.depth = depth;
    }
}
