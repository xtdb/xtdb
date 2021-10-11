package core2.relation;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

@SuppressWarnings("try")
public interface IListWriter extends IColumnWriter<ListVector> {
    @Override
    ListVector getVector();

    <V extends ValueVector> IColumnWriter<V> getDataWriter();
}
