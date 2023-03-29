package xtdb.vector.extensions;

import clojure.lang.Keyword;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class KeywordVector extends XtExtensionVector<VarCharVector> {

    public KeywordVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new VarCharVector(name, allocator));
    }

    public KeywordVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new VarCharVector(field, allocator));
    }

    @Override
    public Keyword getObject(int index) {
        return Keyword.intern(getUnderlyingVector().getObject(index).toString());
    }
}
