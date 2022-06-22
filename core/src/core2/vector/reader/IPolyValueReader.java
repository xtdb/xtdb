package core2.vector.reader;

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
