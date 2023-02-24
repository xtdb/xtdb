package core2.vector;

public interface IPolyValueReader {
    byte read();

    boolean readBoolean();
    byte readByte();
    short readShort();
    int readInt();
    long readLong();

    float readFloat();
    double readDouble();

    Object readObject();
}
