package xtdb.vector;

import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;

public interface IValueWriter {

    void writeNull(Void nullValue);
    void writeBoolean(boolean booleanValue);
    void writeByte(byte byteValue);
    void writeShort(short shortValue);
    void writeInt(int intValue);
    void writeLong(long longValue);
    void writeFloat(float floatValue);
    void writeDouble(double doubleValue);
    void writeBytes(ByteBuffer bytesValue);
    void writeObject(Object objectValue);

    IValueWriter structKeyWriter(String key);
    void structWritten();

    IValueWriter listElementWriter();
    void listWritten();

    IValueWriter writerForType(Object colType);
    byte registerNewType(Field field);
    IValueWriter writerForTypeId(byte typeId);
}
