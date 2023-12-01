package xtdb.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TransitType extends XtExtensionType {
    public static final TransitType INSTANCE = new TransitType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public TransitType() {
        super("xt/transit+msgpack", Binary.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new TransitVector(name, allocator, fieldType);
    }
}
