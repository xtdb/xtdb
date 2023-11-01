package xtdb.vector;

import clojure.lang.Keyword;

import java.nio.ByteBuffer;

public interface IValueWriter {

    void writeNull();

    void writeBoolean(boolean booleanValue);

    void writeByte(byte byteValue);

    void writeShort(short shortValue);

    void writeInt(int intValue);

    void writeLong(long longValue);

    void writeFloat(float floatValue);

    void writeDouble(double doubleValue);

    void writeBytes(ByteBuffer bytesValue);

    void writeObject(Object objectValue);

    IValueWriter legWriter(Keyword leg);
}
