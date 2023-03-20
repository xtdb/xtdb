package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

import java.util.HashSet;
import java.util.Set;

public class SetVector extends XtExtensionVector<ListVector> {
    public SetVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
        super(name, allocator, fieldType, new ListVector(name, allocator, fieldType, callBack));
    }

    public SetVector(String name, BufferAllocator allocator, FieldType fieldType) {
        this(name, allocator, fieldType, null);
    }

    public SetVector(Field field, BufferAllocator allocator, CallBack callBack) {
        this(field.getName(), allocator, field.getFieldType(), callBack);
    }

    public SetVector(Field field, BufferAllocator allocator) {
        this(field, allocator, null);
    }

    @Override
    public Set<?> getObject(int index) {
        return new HashSet<Object>(getUnderlyingVector().getObject(index));
    }

    @Override
    public Field getField() {
        var field = super.getField();
        return new Field(field.getName(), field.getFieldType(), getUnderlyingVector().getField().getChildren());
    }
}
