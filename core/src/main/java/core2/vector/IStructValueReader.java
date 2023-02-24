package core2.vector;

public interface IStructValueReader {

    boolean readBoolean(String fieldName);
    byte readByte(String fieldName);
    short readShort(String fieldName);
    int readInt(String fieldName);
    long readLong(String fieldName);

    float readFloat(String fieldName);
    double readDouble(String fieldName);

    Object readObject(String fieldName);

    IPolyValueReader readField(String fieldName);
}
