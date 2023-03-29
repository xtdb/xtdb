package xtdb.vector.extensions;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ClojureFormVector extends XtExtensionVector<VarCharVector> {

    private static final IFn READ_STRING = Clojure.var("clojure.core", "read-string");
    private static final IFn TO_CLJ_FORM = Clojure.var("xtdb.api", "->ClojureForm");

    public ClojureFormVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new VarCharVector(name, allocator));
    }

    public ClojureFormVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new VarCharVector(field, allocator));
    }

    @Override
    public Object getObject(int index) {
        return TO_CLJ_FORM.invoke(READ_STRING.invoke(getUnderlyingVector().getObject(index).toString()));
    }
}
