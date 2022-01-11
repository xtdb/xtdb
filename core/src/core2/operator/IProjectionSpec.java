package core2.operator;

import core2.vector.IIndirectRelation;
import core2.vector.IIndirectVector;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.Buffer;

public interface IProjectionSpec {
    String getColumnName();

    IIndirectVector<?> project(BufferAllocator allocator, IIndirectRelation readRelation);
}
