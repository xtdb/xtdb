package core2.vector;

public interface IPolyValueWriter {
    void writeNull(byte typeId, Void nullValue);

    void writeBoolean(byte typeId, boolean booleanValue);
    void writeByte(byte typeId, byte byteValue);
    void writeShort(byte typeId, short shortValue);
    void writeInt(byte typeId, int intValue);
    void writeLong(byte typeId, long longValue);

    void writeFloat(byte typeId, float floatValue);
    void writeDouble(byte typeId, double doubleValue);

    void writeObject(byte typeId, Object objectValue);

    IMonoVectorWriter writeMonoListElements(byte typeId, int elementCount);
    IPolyVectorWriter writePolyListElements(byte typeId, int elementCount);

    void writeStructEntries(byte typeId);
    IMonoValueWriter monoStructFieldWriter(byte typeId, String fieldName);
    IPolyValueWriter polyStructFieldWriter(byte typeId, String fieldName);
}
