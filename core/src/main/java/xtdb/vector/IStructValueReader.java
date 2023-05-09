package xtdb.vector;

import java.nio.ByteBuffer;

public interface IStructValueReader {

    boolean readBoolean(String fieldName);
    byte readByte(String fieldName);
    short readShort(String fieldName);
    int readInt(String fieldName);
    long readLong(String fieldName);

    float readFloat(String fieldName);
    double readDouble(String fieldName);

    ByteBuffer readBytes(String fieldName);
    Object readObject(String fieldName);

    IPolyValueReader readField(String fieldName);
}
