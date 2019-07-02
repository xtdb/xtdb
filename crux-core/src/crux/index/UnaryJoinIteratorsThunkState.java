package crux.index;

public class UnaryJoinIteratorsThunkState {
    public Object iterators;
    public long index;

    public UnaryJoinIteratorsThunkState(Object iterators, long index) {
        this.iterators = iterators;
        this.index = index;
    }
}
