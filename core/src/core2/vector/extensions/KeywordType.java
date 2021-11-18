package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class KeywordType extends XtExtensionType {
    public static final KeywordType INSTANCE = new KeywordType();

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public KeywordType() {
        super("keyword", Utf8.INSTANCE);
    }

    @Override
    protected ArrowType deserialize(String serializedData) {
        return INSTANCE;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new KeywordVector(name, allocator, fieldType);
    }
}
