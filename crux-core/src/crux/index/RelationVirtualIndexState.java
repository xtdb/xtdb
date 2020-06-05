package crux.index;

public class RelationVirtualIndexState {
    public Object tree;
    public Object path;
    public Object indexes;
    public Object key;

    public RelationVirtualIndexState(Object tree, Object path, Object indexes, Object key) {
        this.tree = tree;
        this.path = path;
        this.indexes = indexes;
        this.key = key;
    }
}
