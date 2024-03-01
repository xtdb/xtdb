package xtdb.vector;

import clojure.lang.Keyword;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

abstract class BoxWriter implements IValueWriter {
    abstract IValueWriter box();

    @Override
    public void writeNull() {
        box().writeNull();
    }

    @Override
    public void writeBoolean(boolean v) {
        box().writeBoolean(v);
    }

    @Override
    public void writeByte(byte v) {
        box().writeByte(v);
    }

    @Override
    public void writeShort(short v) {
        box().writeShort(v);
    }

    @Override
    public void writeInt(int v) {
        box().writeInt(v);
    }

    @Override
    public void writeLong(long v) {
        box().writeLong(v);
    }

    @Override
    public void writeFloat(float v) {
        box().writeFloat(v);
    }

    @Override
    public void writeDouble(double v) {
        box().writeDouble(v);
    }

    @Override
    public void writeBytes(ByteBuffer v) {
        box().writeBytes(v);
    }

    @Override
    public void writeObject(@Nullable Object obj) {
        box().writeObject(obj);
    }

    @Override
    public IValueWriter legWriter(Keyword leg) {
        return box().legWriter(leg);
    }
}
