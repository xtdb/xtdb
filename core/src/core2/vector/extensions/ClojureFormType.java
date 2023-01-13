package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ClojureFormType extends XtExtensionType {
    public static final ClojureFormType INSTANCE = new ClojureFormType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public ClojureFormType() {
        super("c2/clj-form", Utf8.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new ClojureFormVector(name, allocator, fieldType);
    }
}
