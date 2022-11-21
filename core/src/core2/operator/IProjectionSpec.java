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

    /**
     * @param params a single-row indirect relation containing the params for this invocation - maybe a view over a bigger param relation.
     */
    IIndirectVector<?> project(BufferAllocator allocator, IIndirectRelation readRelation, IIndirectRelation params);
}
