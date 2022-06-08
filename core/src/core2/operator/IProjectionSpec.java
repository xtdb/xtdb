package core2.operator;

import clojure.lang.Symbol;
import core2.vector.IIndirectRelation;
import core2.vector.IIndirectVector;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.Buffer;
import java.util.Map;

public interface IProjectionSpec {
    Symbol getColumnName();
    Object getColumnType();

    IIndirectVector<?> project(BufferAllocator allocator, IIndirectRelation readRelation, Map<Symbol, Object> params);
}
