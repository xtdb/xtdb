package core2.select;

import org.apache.arrow.vector.ValueVector;

public interface IVectorPredicate {
    boolean test(ValueVector vector, int idx);
}
