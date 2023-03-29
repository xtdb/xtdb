package xtdb.vector;

public class ValueBox implements IMonoValueWriter, IPolyValueReader, IPolyValueWriter {
    private static final byte NO_TYPE_ID = Byte.MIN_VALUE;

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
    public byte read() {
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

    @Override
    public void writeNull(byte typeId, Void nullValue) {
        this.typeId = typeId;
    }

    @Override
    public void writeNull(Void nullValue) {
        writeNull(NO_TYPE_ID, nullValue);
    }

    @Override
    public void writeBoolean(byte typeId, boolean booleanValue) {
        this.typeId = typeId;
        this.booleanValue = booleanValue;
    }

    @Override
    public void writeBoolean(boolean booleanValue) {
        writeBoolean(NO_TYPE_ID, booleanValue);
    }

    @Override
    public void writeByte(byte typeId, byte byteValue) {
        this.typeId = typeId;
        this.byteValue = byteValue;
    }

    @Override
    public void writeByte(byte byteValue) {
        writeByte(NO_TYPE_ID, byteValue);
    }

    @Override
    public void writeShort(byte typeId, short shortValue) {
        this.typeId = typeId;
        this.shortValue = shortValue;
    }

    @Override
    public void writeShort(short shortValue) {
        writeShort(NO_TYPE_ID, shortValue);
    }

    @Override
    public void writeInt(byte typeId, int intValue) {
        this.typeId = typeId;
        this.intValue = intValue;
    }

    @Override
    public void writeInt(int intValue) {
        writeInt(NO_TYPE_ID, intValue);
    }

    @Override
    public void writeLong(byte typeId, long longValue) {
        this.typeId = typeId;
        this.longValue = longValue;
    }

    @Override
    public void writeLong(long longValue) {
        writeLong(NO_TYPE_ID, longValue);
    }

    @Override
    public void writeFloat(byte typeId, float floatValue) {
        this.typeId = typeId;
        this.floatValue = floatValue;
    }

    @Override
    public void writeFloat(float floatValue) {
        writeFloat(NO_TYPE_ID, floatValue);
    }

    @Override
    public void writeDouble(byte typeId, double doubleValue) {
        this.typeId = typeId;
        this.doubleValue = doubleValue;
    }

    @Override
    public void writeDouble(double doubleValue) {
        writeDouble(NO_TYPE_ID, doubleValue);
    }

    @Override
    public void writeObject(byte typeId, Object objectValue) {
        this.typeId = typeId;
        this.objectValue = objectValue;
    }

    @Override
    public void writeObject(Object objectValue) {
        writeObject(NO_TYPE_ID, objectValue);
    }

    @Override
    public IMonoVectorWriter writeMonoListElements(byte typeId, int elementCount) {
        this.typeId = typeId;
        ListValueBox listValueBox = new ListValueBox(elementCount);
        this.objectValue = listValueBox;
        return listValueBox;
    }

    @Override
    public IMonoVectorWriter writeMonoListElements(int elementCount) {
        return writeMonoListElements(NO_TYPE_ID, elementCount);
    }

    @Override
    public IPolyVectorWriter writePolyListElements(byte typeId, int elementCount) {
        this.typeId = typeId;
        ListValueBox listValueBox = new ListValueBox(elementCount);
        this.objectValue = listValueBox;
        return listValueBox;
    }

    @Override
    public IPolyVectorWriter writePolyListElements(int elementCount) {
        return writePolyListElements(NO_TYPE_ID, elementCount);
    }

    @Override
    public void writeStructEntries(byte typeId) {
        this.typeId = typeId;
        this.objectValue = new StructValueBox();
    }

    @Override
    public void writeStructEntries() {
        writeStructEntries(NO_TYPE_ID);
    }

    @Override
    public IMonoValueWriter monoStructFieldWriter(byte typeId, String fieldName) {
        this.typeId = typeId;
        return ((StructValueBox) objectValue).monoFieldWriter(fieldName);
    }

    @Override
    public IMonoValueWriter monoStructFieldWriter(String fieldName) {
        return monoStructFieldWriter(NO_TYPE_ID, fieldName);
    }

    @Override
    public IPolyValueWriter polyStructFieldWriter(byte typeId, String fieldName) {
        this.typeId = typeId;
        return ((StructValueBox) objectValue).polyFieldWriter(fieldName);
    }

    @Override
    public IPolyValueWriter polyStructFieldWriter(String fieldName) {
        return polyStructFieldWriter(NO_TYPE_ID, fieldName);
    }
}
