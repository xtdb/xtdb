package xtdb.vector;

import java.nio.ByteBuffer;

public interface IPolyVectorReader extends IPolyValueReader {
    int valueCount();

    byte read(int idx);

    static IPolyVectorReader indirect(IPolyVectorReader inner, IVectorIndirection indirection) {
        return new IPolyVectorReader() {
            @Override
            public byte read() {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte read(int idx) {
                return inner.read(indirection.getIndex(idx));
            }

            @Override
            public boolean readBoolean() {
                return inner.readBoolean();
            }

            @Override
            public byte readByte() {
                return inner.readByte();
            }

            @Override
            public short readShort() {
                return inner.readShort();
            }

            @Override
            public int readInt() {
                return inner.readInt();
            }

            @Override
            public long readLong() {
                return inner.readLong();
            }

            @Override
            public float readFloat() {
                return inner.readFloat();
            }

            @Override
            public double readDouble() {
                return inner.readDouble();
            }

            @Override
            public ByteBuffer readBytes() {
                return inner.readBytes();
            }

            @Override
            public Object readObject() {
                return inner.readObject();
            }

            @Override
            public int valueCount() {
                return inner.valueCount();
            }
        };
    }
}
