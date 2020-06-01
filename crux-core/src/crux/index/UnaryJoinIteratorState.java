package crux.index;

public class UnaryJoinIteratorState {
    public Object idx;
    public Object key;

    public UnaryJoinIteratorState(Object idx, Object key) {
        this.idx = idx;
        this.key = key;
    }
}
