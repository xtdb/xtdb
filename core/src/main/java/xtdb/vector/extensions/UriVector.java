package xtdb.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class UriVector extends XtExtensionVector<VarCharVector> {
    public UriVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new VarCharVector(name, allocator));
    }

    public UriVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new VarCharVector(field, allocator));
    }

    @Override
    public URI getObject(int index) {
        String s = new String(getUnderlyingVector().get(index), StandardCharsets.UTF_8);
        return URI.create(s);
    }
}
