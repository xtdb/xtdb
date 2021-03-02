package core2.select;

import org.apache.arrow.vector.FieldVector;

public interface IVectorCompare {
    int compareIdx(FieldVector fieldVector, int idx);
}
