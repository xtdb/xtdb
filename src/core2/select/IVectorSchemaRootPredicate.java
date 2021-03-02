package core2.select;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface IVectorSchemaRootPredicate {
    boolean test(VectorSchemaRoot root, int idx);
}
