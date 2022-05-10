package core2.expression.boxes;

public class ObjectBox {
    public Object value;

    public ObjectBox withValue(Object value) {
        this.value = value;
        return this;
    }
}
