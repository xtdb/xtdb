package core2.expression.boxes;

public class LongBox {
    public long value;

    public LongBox withValue(long value) {
        this.value = value;
        return this;
    }
}
