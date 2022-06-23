package core2.vector;

import java.nio.ByteBuffer;

public class PolyValueBox implements IPolyValueReader, IPolyValueWriter {
    private byte typeId;

    private boolean booleanValue;
    private byte byteValue;
    private short shortValue;
    private int intValue;
    private long longValue;

    private float floatValue;
    private double doubleValue;

    private ByteBuffer bufferValue;
    private Object objectValue;

    @Override
    public byte getTypeId() {
        return typeId;
    }

    @Override
    public boolean readBoolean() {
        return booleanValue;
    }

    @Override
    public byte readByte() {
        return byteValue;
    }

    @Override
    public short readShort() {
        return shortValue;
    }

    @Override
    public int readInt() {
        return intValue;
    }

    @Override
    public long readLong() {
        return longValue;
    }

    @Override
    public float readFloat() {
        return floatValue;
    }

    @Override
    public double readDouble() {
        return doubleValue;
    }

    @Override
    public ByteBuffer readBuffer() {
        return bufferValue;
    }

    @Override
    public Object readObject() {
        return objectValue;
    }

    @Override
    public void writeNull(byte typeId, Void nullValue) {
        this.typeId = typeId;
    }

    @Override
    public void writeBoolean(byte typeId, boolean booleanValue) {
        this.typeId = typeId;
        this.booleanValue = booleanValue;
    }

    @Override
    public void writeByte(byte typeId, byte byteValue) {
        this.typeId = typeId;
        this.byteValue = byteValue;
    }

    @Override
    public void writeShort(byte typeId, short shortValue) {
        this.typeId = typeId;
        this.shortValue = shortValue;
    }

    @Override
    public void writeInt(byte typeId, int intValue) {
        this.typeId = typeId;
        this.intValue = intValue;
    }

    @Override
    public void writeLong(byte typeId, long longValue) {
        this.typeId = typeId;
        this.longValue = longValue;
    }

    @Override
    public void writeFloat(byte typeId, float floatValue) {
        this.typeId = typeId;
        this.floatValue = floatValue;
    }

    @Override
    public void writeDouble(byte typeId, double doubleValue) {
        this.typeId = typeId;
        this.doubleValue = doubleValue;
    }

    @Override
    public void writeBuffer(byte typeId, ByteBuffer bufferValue) {
        this.typeId = typeId;
        this.bufferValue = bufferValue;
    }

    @Override
    public void writeObject(byte typeId, Object bufferValue) {
        this.typeId = typeId;
        this.objectValue = objectValue;
    }
}
