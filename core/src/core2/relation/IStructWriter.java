package core2.relation;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

@SuppressWarnings("try")
public interface IStructWriter extends IColumnWriter<StructVector> {
    @Override
    StructVector getVector();

    <V extends ValueVector> IColumnWriter<V> writerForName(String columnName);
}
