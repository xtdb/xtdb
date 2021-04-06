package core2.select;

import org.apache.arrow.vector.ValueVector;

public interface IVectorCompare {
    int compareIdx(ValueVector vector, int idx);
}
