package core2.vector;

import core2.relation.IColumnReader;
import org.apache.arrow.vector.ValueVector;

public interface IRowCopier<R extends ValueVector, W extends ValueVector> {
    void copyRow(int sourceIdx);

    IVectorWriter<W> getWriter();
    IColumnReader<R> getReader();
}
