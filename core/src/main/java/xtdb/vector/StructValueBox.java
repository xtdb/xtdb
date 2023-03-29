package xtdb.vector;

import java.util.HashMap;
import java.util.Map;

class StructValueBox implements IStructValueReader {

    private final Map<String, ValueBox> fields = new HashMap<>();

    public IMonoValueWriter monoFieldWriter(String fieldName) {
        return fields.computeIfAbsent(fieldName, s -> new ValueBox());
    }

    public IPolyValueWriter polyFieldWriter(String fieldName) {
        return fields.computeIfAbsent(fieldName, s -> new ValueBox());
    }

    @Override
    public boolean readBoolean(String fieldName) {
        return fields.get(fieldName).readBoolean();
    }

    @Override
    public byte readByte(String fieldName) {
        return fields.get(fieldName).readByte();
    }

    @Override
    public short readShort(String fieldName) {
        return fields.get(fieldName).readShort();
    }

    @Override
    public int readInt(String fieldName) {
        return fields.get(fieldName).readInt();
    }

    @Override
    public long readLong(String fieldName) {
        return fields.get(fieldName).readLong();
    }

    @Override
    public float readFloat(String fieldName) {
        return fields.get(fieldName).readFloat();
    }

    @Override
    public double readDouble(String fieldName) {
        return fields.get(fieldName).readDouble();
    }

    @Override
    public Object readObject(String fieldName) {
        return fields.get(fieldName).readObject();
    }

    @Override
    public IPolyValueReader readField(String fieldName) {
        return fields.get(fieldName);
    }
}
