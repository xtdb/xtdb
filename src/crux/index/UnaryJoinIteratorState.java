package crux.index;

public class UnaryJoinIteratorState {
    public Object idx;
    public Object key;
    public Object results;

    public UnaryJoinIteratorState(Object idx, Object key, Object results) {
        this.idx = idx;
        this.key = key;
        this.results = results;
    }
}
