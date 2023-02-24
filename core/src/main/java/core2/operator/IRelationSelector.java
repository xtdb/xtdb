package core2.operator;

import clojure.lang.Symbol;
import core2.vector.IIndirectRelation;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Map;

public interface IRelationSelector {
    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    int[] select(BufferAllocator allocator, IIndirectRelation readRelation, IIndirectRelation params);
}
