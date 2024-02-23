package xtdb.vector;

import clojure.lang.Keyword;

import java.nio.ByteBuffer;

public interface IValueReader {
    Keyword getLeg();

    boolean isNull();
    boolean isAbsent();

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
