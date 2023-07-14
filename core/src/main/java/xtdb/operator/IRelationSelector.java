package xtdb.operator;

import org.apache.arrow.memory.BufferAllocator;
import xtdb.vector.RelationReader;

public interface IRelationSelector {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    int[] select(BufferAllocator allocator, RelationReader readRelation, RelationReader params);
}
