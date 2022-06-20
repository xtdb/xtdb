package core2.vector.reader;

public interface IPolyValueReader {
    byte getTypeId();

    default boolean readBoolean() {
        throw new UnsupportedOperationException("can't read boolean");
    }

    default byte readByte() {
        throw new UnsupportedOperationException("can't read byte");
    }

    default short readShort() {
        throw new UnsupportedOperationException("can't read short");
    }

    default int readInt() {
        throw new UnsupportedOperationException("can't read int");
    }

    default long readLong() {
        throw new UnsupportedOperationException("can't read long");
    }

    default float readFloat() {
        throw new UnsupportedOperationException("can't read float");
    }

    default double readDouble() {
        throw new UnsupportedOperationException("can't read double");
    }

    default Object readObject() {
        throw new UnsupportedOperationException("can't read Object");
    }
}
