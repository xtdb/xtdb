package xtdb.vector;

class ListValueBox implements IMonoVectorReader, IMonoVectorWriter, IPolyVectorReader, IPolyVectorWriter {

    private final int valueCount;
    private final IWriterPosition wp = IWriterPosition.build();
    private final ValueBox[] els;
    private ValueBox readBox;

    ListValueBox(int valueCount) {
        this.valueCount = valueCount;
        els = new ValueBox[valueCount];
        for (int i = 0; i < els.length; i++) {
            els[i] = new ValueBox();
        }
    }

    @Override
    public int valueCount() {
        return valueCount;
    }

    @Override
    public boolean readBoolean(int idx) {
        return els[idx].readBoolean();
    }

    @Override
    public byte readByte(int idx) {
        return els[idx].readByte();
    }

    @Override
    public short readShort(int idx) {
        return els[idx].readShort();
    }

    @Override
    public int readInt(int idx) {
        return els[idx].readInt();
    }

    @Override
    public long readLong(int idx) {
        return els[idx].readLong();
    }

    @Override
    public float readFloat(int idx) {
        return els[idx].readFloat();
    }

    @Override
    public double readDouble(int idx) {
        return els[idx].readDouble();
    }

    @Override
    public Object readObject(int idx) {
        return els[idx].readObject();
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
    public Object readObject() {
        return readBox.readObject();
    }

    @Override
    public byte read(int idx) {
        this.readBox = els[idx];
        return read();
    }

    @Override
    public IWriterPosition writerPosition() {
        return wp;
    }

    @Override
    public void writeNull(Void nullValue) {
        els[wp.getPositionAndIncrement()].writeNull(nullValue);
    }

    @Override
    public void writeBoolean(boolean booleanValue) {
        els[wp.getPositionAndIncrement()].writeBoolean(booleanValue);
    }

    @Override
    public void writeByte(byte byteValue) {
        els[wp.getPositionAndIncrement()].writeByte(byteValue);
    }

    @Override
    public void writeShort(short shortValue) {
        els[wp.getPositionAndIncrement()].writeShort(shortValue);
    }

    @Override
    public void writeInt(int intValue) {
        els[wp.getPositionAndIncrement()].writeInt(intValue);
    }

    @Override
    public void writeLong(long longValue) {
        els[wp.getPositionAndIncrement()].writeLong(longValue);
    }

    @Override
    public void writeFloat(float floatValue) {
        els[wp.getPositionAndIncrement()].writeFloat(floatValue);
    }

    @Override
    public void writeDouble(double doubleValue) {
        els[wp.getPositionAndIncrement()].writeDouble(doubleValue);
    }

    @Override
    public void writeObject(Object objectValue) {
        els[wp.getPositionAndIncrement()].writeObject(objectValue);
    }

    @Override
    public IMonoVectorWriter writeMonoListElements(int elementCount) {
        return els[wp.getPositionAndIncrement()].writeMonoListElements(elementCount);
    }

    @Override
    public IPolyVectorWriter writePolyListElements(int elementCount) {
        return els[wp.getPositionAndIncrement()].writePolyListElements(elementCount);
    }

    @Override
    public void writeStructEntries() {
        els[wp.getPositionAndIncrement()].writeStructEntries();
    }

    // HACK: gah, I don't like the `- 1`s here but `writeStructEntries` has already incremented it
    // neither can we have these methods increment it without making assumptions about the calling patterns
    // ... which we're kind of doing already, I guess...

    @Override
    public IMonoValueWriter monoStructFieldWriter(String fieldName) {
        return els[wp.getPosition() - 1].monoStructFieldWriter(fieldName);
    }

    @Override
    public IPolyValueWriter polyStructFieldWriter(String fieldName) {
        return els[wp.getPosition() - 1].polyStructFieldWriter(fieldName);
    }

    @Override
    public void writeNull(byte typeId, Void nullValue) {
        els[wp.getPositionAndIncrement()].writeNull(typeId, nullValue);
    }

    @Override
    public void writeBoolean(byte typeId, boolean booleanValue) {
        els[wp.getPositionAndIncrement()].writeBoolean(typeId, booleanValue);
    }

    @Override
    public void writeByte(byte typeId, byte byteValue) {
        els[wp.getPositionAndIncrement()].writeByte(typeId, byteValue);
    }

    @Override
    public void writeShort(byte typeId, short shortValue) {
        els[wp.getPositionAndIncrement()].writeShort(typeId, shortValue);
    }

    @Override
    public void writeInt(byte typeId, int intValue) {
        els[wp.getPositionAndIncrement()].writeInt(typeId, intValue);
    }

    @Override
    public void writeLong(byte typeId, long longValue) {
        els[wp.getPositionAndIncrement()].writeLong(typeId, longValue);
    }

    @Override
    public void writeFloat(byte typeId, float floatValue) {
        els[wp.getPositionAndIncrement()].writeFloat(typeId, floatValue);
    }

    @Override
    public void writeDouble(byte typeId, double doubleValue) {
        els[wp.getPositionAndIncrement()].writeDouble(typeId, doubleValue);
    }

    @Override
    public void writeObject(byte typeId, Object objectValue) {
        els[wp.getPositionAndIncrement()].writeObject(typeId, objectValue);
    }

    @Override
    public IMonoVectorWriter writeMonoListElements(byte typeId, int elementCount) {
        return els[wp.getPositionAndIncrement()].writeMonoListElements(typeId, elementCount);
    }

    @Override
    public IPolyVectorWriter writePolyListElements(byte typeId, int elementCount) {
        return els[wp.getPositionAndIncrement()].writePolyListElements(typeId, elementCount);
    }

    @Override
    public void writeStructEntries(byte typeId) {
        els[wp.getPositionAndIncrement()].writeStructEntries();
    }

    @Override
    public IMonoValueWriter monoStructFieldWriter(byte typeId, String fieldName) {
        return els[wp.getPosition() - 1].monoStructFieldWriter(typeId, fieldName);
    }

    @Override
    public IPolyValueWriter polyStructFieldWriter(byte typeId, String fieldName) {
        return els[wp.getPosition() - 1].polyStructFieldWriter(typeId, fieldName);
    }
}
