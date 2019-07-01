package crux.index;

public class RelationIteratorsState {
    public Object indexes;
    public Object child_idx;
    public boolean needs_seek_QMARK_;

    public RelationIteratorsState(Object indexes, Object child_idx, boolean needs_seek_QMARK_) {
        this.indexes = indexes;
        this.child_idx = child_idx;
        this.needs_seek_QMARK_ = needs_seek_QMARK_;
    }
}
