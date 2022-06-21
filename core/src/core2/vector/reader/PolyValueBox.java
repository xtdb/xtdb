package core2.vector.reader;

public class PolyValueBox implements IPolyValueReader {
    private byte typeId;

    private boolean booleanValue;
    private byte byteValue;
    private short shortValue;
    private int intValue;
    private long longValue;

    private float floatValue;
    private double doubleValue;

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
    public Object readObject() {
        return objectValue;
    }

    public void writeBoolean(byte typeId, boolean booleanValue) {
        this.typeId = typeId;
        this.booleanValue = booleanValue;
    }

    public void writeByte(byte typeId, byte byteValue) {
        this.typeId = typeId;
        this.byteValue = byteValue;
    }

    public void writeShort(byte typeId, short shortValue) {
        this.typeId = typeId;
        this.shortValue = shortValue;
    }

    public void writeInt(byte typeId, int intValue) {
        this.typeId = typeId;
        this.intValue = intValue;
    }

    public void writeLong(byte typeId, long longValue) {
        this.typeId = typeId;
        this.longValue = longValue;
    }

    public void writeFloat(byte typeId, float floatValue) {
        this.typeId = typeId;
        this.floatValue = floatValue;
    }

    public void writeDouble(byte typeId, double doubleValue) {
        this.typeId = typeId;
        this.doubleValue = doubleValue;
    }

    public void writeObject(byte typeId, Object objectValue) {
        this.typeId = typeId;
        this.objectValue = objectValue;
    }
}
