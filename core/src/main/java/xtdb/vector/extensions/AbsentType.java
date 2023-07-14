package xtdb.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class AbsentType extends XtExtensionType {
    public static final AbsentType INSTANCE = new AbsentType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public AbsentType() {
        super("absent", Null.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new AbsentVector(name, allocator, fieldType);
    }
}
