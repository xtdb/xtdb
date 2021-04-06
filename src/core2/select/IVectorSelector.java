package core2.select;

import org.apache.arrow.vector.ValueVector;
import org.roaringbitmap.RoaringBitmap;

public interface IVectorSelector {
    RoaringBitmap select(ValueVector vector);
}
