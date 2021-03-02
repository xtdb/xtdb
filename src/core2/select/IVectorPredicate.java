package core2.select;

import org.apache.arrow.vector.FieldVector;

public interface IVectorPredicate {
    boolean test(FieldVector fieldVector, int idx);
}
