package core2.vector;

import java.nio.ByteBuffer;

public interface IPolyValueWriter {
    void writeNull(byte typeId, Void nullValue);

    void writeBoolean(byte typeId, boolean booleanValue);
    void writeByte(byte typeId, byte byteValue);
    void writeShort(byte typeId, short shortValue);
    void writeInt(byte typeId, int intValue);
    void writeLong(byte typeId, long longValue);

    void writeFloat(byte typeId, float floatValue);
    void writeDouble(byte typeId, double doubleValue);

    void writeBuffer(byte typeId, ByteBuffer bufferValue);
    void writeObject(byte typeId, Object objectValue);
}
