package core2.relation;

import org.apache.arrow.vector.ValueVector;

public interface IRowCopier<R extends ValueVector, W extends ValueVector> {
    void copyRow(int sourceIdx);

    IColumnWriter<W> getWriter();
    IColumnReader<R> getReader();
}
