package core2.operator;

import core2.vector.IIndirectRelation;
import org.apache.arrow.memory.BufferAllocator;

public interface IRelationSelector {
    int[] select(BufferAllocator allocator, IIndirectRelation readRelation);
}
