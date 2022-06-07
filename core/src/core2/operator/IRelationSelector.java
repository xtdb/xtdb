package core2.operator;

import clojure.lang.Symbol;
import core2.vector.IIndirectRelation;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Map;

public interface IRelationSelector {
    int[] select(BufferAllocator allocator, IIndirectRelation readRelation, Map<Symbol, Object> params);
}
