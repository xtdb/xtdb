package xtdb.vector;

import java.nio.ByteBuffer;

public interface IMonoVectorReader {

    int valueCount();

    boolean readBoolean(int idx);
    byte readByte(int idx);
    short readShort(int idx);
    int readInt(int idx);
    long readLong(int idx);

    float readFloat(int idx);
    double readDouble(int idx);

    ByteBuffer readBytes(int idx);
    Object readObject(int idx);

    static IMonoVectorReader indirect(IMonoVectorReader inner, IVectorIndirection indirection) {
        return new IMonoVectorReader() {
            @Override
            public int valueCount() {
                return indirection.valueCount();
            }

            @Override
            public boolean readBoolean(int idx) {
                return inner.readBoolean(indirection.getIndex(idx));
            }

            @Override
            public byte readByte(int idx) {
                return inner.readByte(indirection.getIndex(idx));
            }

            @Override
            public short readShort(int idx) {
                return inner.readShort(indirection.getIndex(idx));
            }

            @Override
            public int readInt(int idx) {
                return inner.readInt(indirection.getIndex(idx));
            }

            @Override
            public long readLong(int idx) {
                return inner.readLong(indirection.getIndex(idx));
            }

            @Override
            public float readFloat(int idx) {
                return inner.readLong(indirection.getIndex(idx));
            }

            @Override
            public double readDouble(int idx) {
                return inner.readDouble(indirection.getIndex(idx));
            }

            @Override
            public ByteBuffer readBytes(int idx) {
                return inner.readBytes(indirection.getIndex(idx));
            }

            @Override
            public Object readObject(int idx) {
                return inner.readObject(indirection.getIndex(idx));
            }
        };
    }
}
