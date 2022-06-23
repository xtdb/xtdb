package core2.vector;

import java.nio.ByteBuffer;

public interface IPolyValueReader {
    byte getTypeId();

    boolean readBoolean();
    byte readByte();
    short readShort();
    int readInt();
    long readLong();

    float readFloat();
    double readDouble();

    ByteBuffer readBuffer();
    Object readObject();
}
