package xtdb.vector.extensions;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.cognitect.transit.Reader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.ByteArrayInputStream;

public class TransitVector extends XtExtensionVector<VarBinaryVector> {

    private static final IFn TRANSIT_MSGPACK_READER = Clojure.var("xtdb.serde", "transit-msgpack-reader");

    public TransitVector(String name, BufferAllocator allocator, FieldType fieldType) {
        super(name, allocator, fieldType, new VarBinaryVector(name, allocator));
    }

    public TransitVector(Field field, BufferAllocator allocator) {
        super(field, allocator, new VarBinaryVector(field, allocator));
    }

    private Reader transitReader(byte[] v) {
        return (Reader) TRANSIT_MSGPACK_READER.invoke(new ByteArrayInputStream(v));
    }

    @Override
    public Object getObject(int index) {
        byte[] v = getUnderlyingVector().get(index);
        return transitReader(v).read();
    }
}
