package xtdb.trie;

import org.apache.arrow.memory.util.ArrowBufPointer;
import xtdb.vector.IRowCopier;
import xtdb.vector.IVectorReader;
import xtdb.vector.RelationReader;

public class CompactorDataRowPointer implements IDataRowPointer {

    public int idx;
    public final RelationReader relReader;
    public final IVectorReader iidReader;
    public final IVectorReader sysFromReader;
    public final IRowCopier rowCopier;

    public CompactorDataRowPointer(RelationReader relReader, IRowCopier rowCopier) {
        this.relReader = relReader;
        iidReader = relReader.readerForName("xt$iid");
        sysFromReader = relReader.readerForName("xt$system_from");

        this.rowCopier = rowCopier;
    }

    @Override
    public ArrowBufPointer getIidPointer(ArrowBufPointer reuse) {
        return iidReader.getPointer(idx, reuse);
    }

    @Override
    public long getSystemTime() {
        return sysFromReader.getLong(idx);
    }

    @Override
    public int getIndex() {
        return idx;
    }

    @Override
    public int nextIndex() {
        return ++idx;
    }

    @Override
    public int rowCount() {
        return relReader.rowCount();
    }
}
