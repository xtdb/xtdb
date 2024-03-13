package xtdb.vector;

import clojure.lang.Keyword;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class ValueBox implements IValueWriter, IValueReader {
    private static final Keyword NULL_LEG = Keyword.intern("null");
    private static final Keyword ABSENT_LEG = Keyword.intern("absent");

    private Keyword leg;

    private long prim;
    private Object obj;

    @Override
    public Keyword getLeg() {
        return leg;
    }

    @Override
    public boolean isNull() {
        return leg == NULL_LEG;
    }

    @Override
    public boolean readBoolean() {
        return prim != 0;
    }

    @Override
    public byte readByte() {
        return (byte) prim;
    }

    @Override
    public short readShort() {
        return (short) prim;
    }

    @Override
    public int readInt() {
        return (int) prim;
    }

    @Override
    public long readLong() {
        return prim;
    }

    @Override
    public float readFloat() {
        return (float) readDouble();
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(prim);
    }

    @Override
    public ByteBuffer readBytes() {
        return ((ByteBuffer) obj);
    }

    @Override
    public Object readObject() {
        return obj;
    }

    @Override
    public void writeNull() {
        obj = null;
    }

    @Override
    public void writeBoolean(boolean v) {
        this.prim = v ? 1 : 0;
    }

    @Override
    public void writeByte(byte v) {
        this.prim = v;
    }

    @Override
    public void writeShort(short v) {
        this.prim = v;
    }

    @Override
    public void writeInt(int v) {
        this.prim = v;
    }

    @Override
    public void writeLong(long v) {
        this.prim = v;
    }

    @Override
    public void writeFloat(float v) {
        writeDouble(v);
    }

    @Override
    public void writeDouble(double v) {
        this.prim = Double.doubleToLongBits(v);
    }

    @Override
    public void writeBytes(ByteBuffer v) {
        this.obj = v;
    }

    @Override
    public void writeObject(@Nullable Object obj) {
        this.obj = obj;
    }

    @Override
    public IValueWriter legWriter(Keyword leg) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                ValueBox.this.leg = leg;
                return ValueBox.this;
            }
        };
    }

}
