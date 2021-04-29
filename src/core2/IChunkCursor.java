package core2;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public interface IChunkCursor extends ICursor<VectorSchemaRoot> {
    Schema getSchema();
}
