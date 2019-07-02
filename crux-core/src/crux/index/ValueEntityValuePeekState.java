package crux.index;

public class ValueEntityValuePeekState {
    public Object last_k;
    public Object value;

    public ValueEntityValuePeekState(Object last_k, Object value) {
        this.last_k = last_k;
        this.value = value;
    }
}
