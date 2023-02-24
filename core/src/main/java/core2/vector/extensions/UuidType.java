package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class UuidType extends XtExtensionType {
    public static final UuidType INSTANCE = new UuidType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public UuidType() {
        super("uuid", new FixedSizeBinary(16));
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new UuidVector(name, allocator, fieldType);
    }
}
