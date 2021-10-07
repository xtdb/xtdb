package core2;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;

import java.lang.reflect.Field;

public class DenseUnionUtil {

    private static final Field DUV_COUNT_FIELD;

    static {
        try {
            Field countField = DenseUnionVector.class.getDeclaredField("valueCount");
            countField.setAccessible(true);
            DUV_COUNT_FIELD = countField;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Can't get DUV 'valueCount' field", e);
        }
    }

    public static void setValueCount(DenseUnionVector duv, int valueCount) {
        try {
            DUV_COUNT_FIELD.setInt(duv, valueCount);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can't access DUV 'valueCount' field.", e);
        }
    }

    public static void setValueCount(NonNullableStructVector vec, int valueCount) {
        for (ValueVector childVec : vec.getChildrenFromFields()) {
            setValueCount(childVec, valueCount);
        }

        vec.valueCount = valueCount;
    }

    public static void setValueCount(ValueVector vec, int valueCount) {
        if (vec instanceof DenseUnionVector) {
            setValueCount(((DenseUnionVector) vec), valueCount);
        } else if(vec instanceof NonNullableStructVector && vec.getNullCount() == 0) {
            setValueCount(((NonNullableStructVector) vec), valueCount);
        } else {
            vec.setValueCount(valueCount);
        }
    }

    public static int writeTypeId(DenseUnionVector duv, int idx, byte typeId) {
        // NOTE: also updates value count of the vector.
        var subVec = duv.getVectorByType(typeId);
        var offset = subVec.getValueCount();

        duv.setTypeId(idx, typeId);
        duv.setOffset(idx, offset);

        setValueCount(subVec, offset + 1);
        setValueCount(duv, idx + 1);

        return offset;
    }
}
