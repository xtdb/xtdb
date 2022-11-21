package core2.vector;

import java.nio.ByteBuffer;

public interface IMonoVectorWriter {

    IWriterPosition writerPosition();

    void writeNull(Void nullValue);

    void writeBoolean(boolean booleanValue);
    void writeByte(byte byteValue);
    void writeShort(short shortValue);
    void writeInt(int intValue);
    void writeLong(long longValue);

    void writeFloat(float floatValue);
    void writeDouble(double doubleValue);

    void writeObject(Object objectValue);
}
