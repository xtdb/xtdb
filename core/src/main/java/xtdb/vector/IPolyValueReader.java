package xtdb.vector;

import java.nio.ByteBuffer;

public interface IPolyValueReader {
    byte read();

    boolean readBoolean();
    byte readByte();
    short readShort();
    int readInt();
    long readLong();

    float readFloat();
    double readDouble();

    ByteBuffer readBytes();
    Object readObject();
}
