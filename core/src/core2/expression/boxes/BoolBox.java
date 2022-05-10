package core2.expression.boxes;

public class BoolBox {
    public boolean value;

    public BoolBox withValue(boolean value) {
        this.value = value;
        return this;
    }
}
