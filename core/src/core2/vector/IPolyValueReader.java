package core2.vector;

public interface IPolyValueReader {
    byte getTypeId();

    boolean readBoolean();
    byte readByte();
    short readShort();
    int readInt();
    long readLong();

    float readFloat();
    double readDouble();

    Object readObject();
}
