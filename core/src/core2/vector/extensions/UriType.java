package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class UriType extends XtExtensionType {
    public static final UriType INSTANCE = new UriType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public UriType() {
        super("uri", Utf8.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new UriVector(name, allocator, fieldType);
    }
}
