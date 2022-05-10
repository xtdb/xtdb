package core2.expression.boxes;

public class DoubleBox {
    public double value;

    public DoubleBox withValue(double value) {
        this.value = value;
        return this;
    }
}
