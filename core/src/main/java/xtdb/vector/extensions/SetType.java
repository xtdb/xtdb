package xtdb.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class SetType extends XtExtensionType {
    public static final SetType INSTANCE = new SetType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public SetType() {
        super("set", List.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new SetVector(name, allocator, fieldType);
    }
}
