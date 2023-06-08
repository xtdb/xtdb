package xtdb.vector;

import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ListValueBox implements IValueWriter, IMonoVectorReader, IPolyVectorReader {

    private final IWriterPosition wp = IWriterPosition.build();
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
    public boolean readBoolean(int idx) {
        return els.get(idx).readBoolean();
    }

    @Override
    public byte readByte(int idx) {
        return els.get(idx).readByte();
    }

    @Override
    public short readShort(int idx) {
        return els.get(idx).readShort();
    }

    @Override
    public int readInt(int idx) {
        return els.get(idx).readInt();
    }

    @Override
    public long readLong(int idx) {
        return els.get(idx).readLong();
    }

    @Override
    public float readFloat(int idx) {
        return els.get(idx).readFloat();
    }

    @Override
    public double readDouble(int idx) {
        return els.get(idx).readDouble();
    }

    @Override
    public ByteBuffer readBytes(int idx) {
        return els.get(idx).readBytes();
    }

    @Override
    public Object readObject(int idx) {
        return els.get(idx).readObject();
    }

    @Override
    public byte read() {
        return readBox.read();
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
    public byte read(int idx) {
        this.readBox = els.get(idx);
        return read();
    }

    private ValueBox newEl() {
        var vb = new ValueBox();
        els.add(wp.getPositionAndIncrement(), vb);
        return vb;
    }

    @Override
    public void writeNull(Void nullValue) {
        newEl().writeNull(nullValue);
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
    public IValueWriter structKeyWriter(String key, Object colType) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return writeBox.structKeyWriter(key, colType);
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
    public IValueWriter writerForType(Object colType) {
        return writeBox.writerForType(colType);
    }

    @Override
    public byte registerNewType(Field field) {
        return writeBox.registerNewType(field);
    }

    @Override
    public IValueWriter writerForTypeId(byte typeId) {
        return writeBox.writerForTypeId(typeId);
    }
}
