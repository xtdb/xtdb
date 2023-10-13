package xtdb.trie;

import clojure.lang.Keyword;
import org.apache.arrow.memory.util.ArrowBufPointer;
import xtdb.vector.IVectorReader;
import xtdb.vector.RelationReader;

import java.util.Comparator;

public class EventRowPointer {

    private int idx;

    public final RelationReader relReader;

    public final IVectorReader iidReader;
    public final IVectorReader sysFromReader;
    public final IVectorReader opReader;

    public final IVectorReader putValidFromReader;
    public final IVectorReader putValidToReader;

    public final IVectorReader deleteValidFromReader;
    public final IVectorReader deleteValidToReader;

    public static final Keyword PUT = Keyword.intern("put");
    public static final Keyword DELETE = Keyword.intern("delete");

    public EventRowPointer(RelationReader relReader, byte[] path) {
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

        int left = 0;
        int right = relReader.rowCount();
        int mid;
        while(left < right) {
            mid = (left + right) / 2;
            if (HashTrie.compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1;
            else right = mid;
        }
        this.idx = left;
    }

    public int getIndex() {
        return idx;
    }

    public int nextIndex() {
        return ++idx;
    }

    public ArrowBufPointer getIidPointer(ArrowBufPointer reuse) {
        return iidReader.getPointer(idx, reuse);
    }

    public long getSystemTime() {
        return sysFromReader.getLong(idx);
    }

    public static Comparator<? extends EventRowPointer> comparator() {
        var leftCmp = new ArrowBufPointer();
        var rightCmp = new ArrowBufPointer();

        return (l, r) -> {
            int cmp = l.getIidPointer(leftCmp).compareTo(r.getIidPointer(rightCmp));
            if (cmp != 0) return cmp;
            return Long.compare(r.getSystemTime(), l.getSystemTime());
        };
    }

    public boolean isValid(ArrowBufPointer reuse, byte[] path) {
        return idx < relReader.rowCount() && HashTrie.compareToPath(getIidPointer(reuse), path) <= 0;
    }
}
