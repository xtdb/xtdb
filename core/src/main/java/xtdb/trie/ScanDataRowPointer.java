package xtdb.trie;

import clojure.lang.Keyword;
import org.apache.arrow.memory.util.ArrowBufPointer;
import xtdb.vector.IVectorReader;
import xtdb.vector.RelationReader;

public class ScanDataRowPointer implements IDataRowPointer {

    private int idx;

    public final RelationReader relReader;

    public final IVectorReader iidReader;
    public final IVectorReader sysFromReader;
    public final IVectorReader opReader;

    public final IVectorReader putValidFromReader;
    public final IVectorReader putValidToReader;

    public final IVectorReader deleteValidFromReader;
    public final IVectorReader deleteValidToReader;

    public final Object rowConsumer;

    public static final Keyword PUT = Keyword.intern("put");
    public static final Keyword DELETE = Keyword.intern("delete");

    public ScanDataRowPointer(RelationReader relReader, Object rowConsumer, byte [] path) {
        this.relReader = relReader;

        iidReader = relReader.readerForName("xt$iid");
        sysFromReader = relReader.readerForName("xt$system_from");
        opReader = relReader.readerForName("op");

        var putReader = opReader.legReader(PUT);
        putValidFromReader = putReader.structKeyReader("xt$valid_from");
        putValidToReader = putReader.structKeyReader("xt$valid_to");

        var deleteReader = opReader.legReader(DELETE);
        deleteValidFromReader = deleteReader.structKeyReader("xt$valid_from");
        deleteValidToReader = deleteReader.structKeyReader("xt$valid_to");

        this.rowConsumer = rowConsumer;

        ArrowBufPointer bufPointer;
        int left = 0;
        int right = rowCount();
        int mid;
        while(left < right) {
            mid = (left + right) / 2;
            if (HashTrie.compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1;
            else right = mid;
        }
        this.idx = left;
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
    public ArrowBufPointer getIidPointer(ArrowBufPointer reuse) {
        return iidReader.getPointer(idx, reuse);
    }

    @Override
    public long getSystemTime() {
        return sysFromReader.getLong(idx);
    }

    @Override
    public int rowCount() {
        return relReader.rowCount();
    }
}
