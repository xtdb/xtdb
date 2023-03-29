package xtdb.operator;

import clojure.lang.Symbol;
import xtdb.vector.IIndirectRelation;
import xtdb.vector.IIndirectVector;
import org.apache.arrow.memory.BufferAllocator;

public interface IProjectionSpec {
    Symbol getColumnName();
    Object getColumnType();

    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    IIndirectVector<?> project(BufferAllocator allocator, IIndirectRelation readRelation, IIndirectRelation params);
}
