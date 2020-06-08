package crux.index;

public class IndexStorePeekState {
    public Object seq;
    public Object key;

    public IndexStorePeekState(Object seq, Object key) {
        this.seq = seq;
        this.key = key;
    }
}
