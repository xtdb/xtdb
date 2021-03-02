package core2.select;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.roaringbitmap.RoaringBitmap;

public interface IVectorSchemaRootSelector {
    RoaringBitmap select(VectorSchemaRoot root);
}
