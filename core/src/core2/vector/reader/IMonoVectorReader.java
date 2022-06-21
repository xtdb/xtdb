package core2.vector.reader;

import org.apache.arrow.memory.ArrowBuf;

public interface IMonoVectorReader {

    default boolean readBoolean(int idx) {
        throw new UnsupportedOperationException("can't read boolean");
    }

    default byte readByte(int idx) {
        throw new UnsupportedOperationException("can't read byte");
    }

    default short readShort(int idx) {
        throw new UnsupportedOperationException("can't read short");
    }

    default int readInt(int idx) {
        throw new UnsupportedOperationException("can't read int");
    }

    default long readLong(int idx) {
        throw new UnsupportedOperationException("can't read long");
    }

    default float readFloat(int idx) {
        throw new UnsupportedOperationException("can't read float");
    }

    default double readDouble(int idx) {
        throw new UnsupportedOperationException("can't read double");
    }

    default Object readObject(int idx) {
        throw new UnsupportedOperationException("can't read Object");
    }
}
