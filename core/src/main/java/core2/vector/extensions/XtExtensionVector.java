package core2.vector.extensions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

public abstract class XtExtensionVector<V extends FieldVector> extends ExtensionTypeVector<V> {
    private final Field field;

    public XtExtensionVector(String name, BufferAllocator allocator, FieldType fieldType, V underlyingVector) {
        super(name, allocator, underlyingVector);
        this.field = new Field(name, fieldType, null);
    }

    public XtExtensionVector(Field field, BufferAllocator allocator, V underlyingVector) {
        super(field, allocator, underlyingVector);
        this.field = field;
    }

    @Override
    public Field getField() {
        return field;
    }

    @Override
    public int hashCode(int index) {
        return getUnderlyingVector().hashCode(index);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
        return getUnderlyingVector().hashCode(index, hasher);
    }

    @Override
    public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
        getUnderlyingVector().copyFromSafe(fromIndex, thisIndex, ((XtExtensionVector<?>) from).getUnderlyingVector());
    }

    @Override
    public TransferPair makeTransferPair(ValueVector target) {
        ValueVector targetUnderlying = ((XtExtensionVector<?>) target).getUnderlyingVector();
        TransferPair tp = getUnderlyingVector().makeTransferPair(targetUnderlying);

        return new TransferPair() {
            @Override
            public void transfer() {
                tp.transfer();
            }

            @Override
            public void splitAndTransfer(int startIndex, int length) {
                tp.splitAndTransfer(startIndex, length);
            }

            @Override
            public ValueVector getTo() {
                return target;
            }

            @Override
            public void copyValueSafe(int from, int to) {
                tp.copyValueSafe(from, to);
            }
        };
    }
}
