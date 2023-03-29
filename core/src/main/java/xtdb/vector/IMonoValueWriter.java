package xtdb.vector;

public interface IMonoValueWriter {
    void writeNull(Void nullValue);

    void writeBoolean(boolean booleanValue);

    void writeByte(byte byteValue);

    void writeShort(short shortValue);

    void writeInt(int intValue);

    void writeLong(long longValue);

    void writeFloat(float floatValue);

    void writeDouble(double doubleValue);

    void writeObject(Object objectValue);

    IMonoVectorWriter writeMonoListElements(int elementCount);

    IPolyVectorWriter writePolyListElements(int elementCount);

    void writeStructEntries();

    IMonoValueWriter monoStructFieldWriter(String fieldName);

    IPolyValueWriter polyStructFieldWriter(String fieldName);
}
