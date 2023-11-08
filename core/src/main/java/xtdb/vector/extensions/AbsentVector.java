package xtdb.vector.extensions;

import clojure.lang.Keyword;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class AbsentVector extends XtExtensionVector<NullVector> {
    private static final Keyword ABSENT_KEYWORD = Keyword.intern("xtdb", "absent");

    public AbsentVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new NullVector(name));
    }

    public AbsentVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new NullVector(field));
    }

    @Override
    public boolean isNull(int index) {
        return false;
    }

    @Override
    public Object getObject(int index) {
        return ABSENT_KEYWORD;
    }
}
