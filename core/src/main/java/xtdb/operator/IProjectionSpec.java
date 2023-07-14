package xtdb.operator;

import clojure.lang.Symbol;
import org.apache.arrow.memory.BufferAllocator;
import xtdb.vector.IVectorReader;
import xtdb.vector.RelationReader;

public interface IProjectionSpec {
    Symbol getColumnName();
    Object getColumnType();

    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    IVectorReader project(BufferAllocator allocator, RelationReader readRelation, RelationReader params);
}
