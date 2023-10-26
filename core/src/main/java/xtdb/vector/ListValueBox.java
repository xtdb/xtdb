package xtdb.vector;

import clojure.lang.Keyword;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ListValueBox implements IValueWriter, IPolyVectorReader {

    private final IVectorPosition wp = IVectorPosition.build();
    private final List<ValueBox> els;
    private ValueBox readBox;
    private ValueBox writeBox;

    ListValueBox() {
        els = new ArrayList<>();
    }

    @Override
    public int valueCount() {
        return wp.getPosition();
    }

    @Override
    public Keyword getLeg() {
        return readBox.getLeg();
    }

    @Override
    public boolean isNull() {
        return readBox.isNull();
    }

    @Override
    public boolean readBoolean() {
        return readBox.readBoolean();
    }

    @Override
    public byte readByte() {
        return readBox.readByte();
    }

    @Override
    public short readShort() {
        return readBox.readShort();
    }

    @Override
    public int readInt() {
        return readBox.readInt();
    }

    @Override
    public long readLong() {
        return readBox.readLong();
    }

    @Override
    public float readFloat() {
        return readBox.readFloat();
    }

    @Override
    public double readDouble() {
        return readBox.readDouble();
    }

    @Override
    public ByteBuffer readBytes() {
        return readBox.readBytes();
    }

    @Override
    public Object readObject() {
        return readBox.readObject();
    }

    @Override
    public void read(int idx) {
        this.readBox = els.get(idx);
    }

    private ValueBox newEl() {
        var vb = new ValueBox();
        els.add(wp.getPositionAndIncrement(), vb);
        return vb;
    }

    @Override
    public void writeNull() {
        newEl().writeNull();
    }

    @Override
    public void writeBoolean(boolean booleanValue) {
        newEl().writeBoolean(booleanValue);
    }

    @Override
    public void writeByte(byte byteValue) {
        newEl().writeByte(byteValue);
    }

    @Override
    public void writeShort(short shortValue) {
        newEl().writeShort(shortValue);
    }

    @Override
    public void writeInt(int intValue) {
        newEl().writeInt(intValue);
    }

    @Override
    public void writeLong(long longValue) {
        newEl().writeLong(longValue);
    }

    @Override
    public void writeFloat(float floatValue) {
        newEl().writeFloat(floatValue);
    }

    @Override
    public void writeDouble(double doubleValue) {
        newEl().writeDouble(doubleValue);
    }

    @Override
    public void writeBytes(ByteBuffer bytesValue) {
        newEl().writeBytes(bytesValue);
    }

    @Override
    public void writeObject(Object objectValue) {
        newEl().writeObject(objectValue);
    }

    @Override
    public IValueWriter listElementWriter() {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return writeBox.listElementWriter();
            }
        };
    }

    @Override
    public void startList() {
        writeBox = new ValueBox();
        els.add(wp.getPosition(), writeBox);
        writeBox.startList();
    }

    @Override
    public void endList() {
        wp.getPositionAndIncrement();
        writeBox.endList();
        writeBox = null;
    }

    @Override
    public IValueWriter structKeyWriter(String key) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return writeBox.structKeyWriter(key);
            }
        };
    }

    @Override
    public void startStruct() {
        writeBox = new ValueBox();
        els.add(wp.getPosition(), writeBox);
        writeBox.startStruct();
    }

    @Override
    public void endStruct() {
        writeBox.endStruct();
        wp.getPositionAndIncrement();
        writeBox = null;
    }

    @Override
    public IValueWriter legWriter(Keyword leg) {
        return writeBox.legWriter(leg);
    }

}
