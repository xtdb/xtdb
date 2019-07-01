package crux.index;

public class RelationNestedIndexState {
    public Object value;
    public Object child_idx;

    public RelationNestedIndexState(Object value, Object child_idx) {
        this.value = value;
        this.child_idx = child_idx;
    }
}
