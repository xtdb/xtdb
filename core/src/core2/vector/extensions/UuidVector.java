package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidVector extends XtExtensionVector<FixedSizeBinaryVector> {
    public UuidVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new FixedSizeBinaryVector(name, allocator, 16));
    }

    public UuidVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new FixedSizeBinaryVector(field, allocator));
    }

    @Override
    public UUID getObject(int index) {
        ByteBuffer bb = ByteBuffer.wrap(getUnderlyingVector().getObject(index));
        return new UUID(bb.getLong(), bb.getLong());
    }
}
