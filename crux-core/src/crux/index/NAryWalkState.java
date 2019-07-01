package crux.index;

public class NAryWalkState {
    public Object result_stack;
    public Object last;

    public NAryWalkState(Object result_stack, Object last) {
        this.result_stack = result_stack;
        this.last = last;
    }
}
