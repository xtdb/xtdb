package xtdb.operator;

import xtdb.vector.IIndirectRelation;
import org.apache.arrow.memory.BufferAllocator;

public interface IRelationSelector {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    int[] select(BufferAllocator allocator, IIndirectRelation readRelation, IIndirectRelation params);
}
