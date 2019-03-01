package crux.index;

public class EntityValueEntityPeekState {
    public Object last_k;
    public Object entity_tx;

    public EntityValueEntityPeekState(Object last_k, Object entity_tx) {
        this.last_k = last_k;
        this.entity_tx = entity_tx;
    }
}
