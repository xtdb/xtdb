package xtdb.vector;

import clojure.lang.Keyword;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.ByteBuffer;

public class ValueBox implements IValueWriter, IPolyValueReader {
    private byte typeId = -1;

    private long prim;
    private Object obj;

    @Override
    public byte read() {
        return typeId;
    }

    @Override
    public boolean readBoolean() {
        return prim != 0;
    }

    @Override
    public byte readByte() {
        return (byte) prim;
    }

    @Override
    public short readShort() {
        return (short) prim;
    }

    @Override
    public int readInt() {
        return (int) prim;
    }

    @Override
    public long readLong() {
        return prim;
    }

    @Override
    public float readFloat() {
        return (float) readDouble();
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(prim);
    }

    @Override
    public ByteBuffer readBytes() {
        return ((ByteBuffer) obj);
    }

    @Override
    public Object readObject() {
        return obj;
    }

    @Override
    public void writeNull(Void nullValue) {}

    @Override
    public void writeBoolean(boolean booleanValue) {
        this.prim = booleanValue ? 1 : 0;
    }

    @Override
    public void writeByte(byte byteValue) {
        this.prim = byteValue;
    }

    @Override
    public void writeShort(short shortValue) {
        this.prim = shortValue;
    }

    @Override
    public void writeInt(int intValue) {
        this.prim = intValue;
    }

    @Override
    public void writeLong(long longValue) {
        this.prim = longValue;
    }

    @Override
    public void writeFloat(float floatValue) {
        writeDouble(floatValue);
    }

    @Override
    public void writeDouble(double doubleValue) {
        this.prim = Double.doubleToLongBits(doubleValue);
    }

    @Override
    public void writeBytes(ByteBuffer bytesValue) {
        this.obj = bytesValue;
    }

    @Override
    public void writeObject(Object objectValue) {
        this.obj = objectValue;
    }

    @Override
    public IValueWriter structKeyWriter(String key) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return ((StructValueBox) obj).fieldWriter(key);
            }
        };
    }

    @Override
    public IValueWriter structKeyWriter(String key, FieldType fieldType) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return ((StructValueBox) obj).fieldWriter(key);
            }
        };
    }

    @Override
    public void startStruct() {
        obj = new StructValueBox();
    }

    @Override
    public void endStruct() {
    }

    @Override
    public IValueWriter listElementWriter() {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return ((ListValueBox) obj);
            }
        };
    }

    @Override
    public IValueWriter listElementWriter(FieldType fieldType) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                return ((ListValueBox) obj);
            }
        };
    }

    @Override
    public void startList() {
        obj = new ListValueBox();
    }

    @Override
    public void endList() {
    }

    @Override
    public IValueWriter legWriter(ArrowType arrowType) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public IValueWriter legWriter(Keyword leg) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public IValueWriter legWriter(Keyword leg, FieldType fieldType) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    @Deprecated
    public IValueWriter writerForTypeId(byte typeId) {
        return new BoxWriter() {
            @Override
            IValueWriter box() {
                ValueBox.this.typeId = typeId;
                return ValueBox.this;
            }
        };
    }
}
