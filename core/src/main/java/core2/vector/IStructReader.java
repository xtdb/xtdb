package core2.vector;

import java.util.Set;

public interface IStructReader {
    Set<String> structKeys();

    IIndirectVector<?> readerForKey(String colName);
}
