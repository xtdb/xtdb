package xtdb.vector;

import clojure.lang.Keyword;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Map;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<Map.Entry<String, IVectorWriter>> {

    /**
     * <p>Maintains the next position to be written to.</p>
     *
     * <p>This is incremented either by using the {@link IRelationWriter#rowCopier}, or by explicitly calling {@link IRelationWriter#endRow()}</p>
     */
    IVectorPosition writerPosition();

    void startRow();
    void endRow();

    /**
     * This method syncs the value counts on the underlying writers/root (e.g. {@link org.apache.arrow.vector.VectorSchemaRoot#setRowCount})
     * so that all of the values written become visible through the Arrow Java API.
     * We don't call this after every write because (for composite vectors, and especially unions) it's not the cheapest call.
     */
    default void syncRowCount() {
        for (Map.Entry<String, IVectorWriter> entry : this) {
            entry.getValue().syncValueCount();
        }
    }

    IVectorWriter colWriter(String colName);

    @Deprecated
    IVectorWriter colWriter(Field field);
    IVectorWriter colWriter(String colName, FieldType fieldType);

    IRowCopier rowCopier(RelationReader relation);

    default void clear() {
        for (Map.Entry<String, IVectorWriter> entry : this) {
            entry.getValue().clear();
        }

        writerPosition().setPosition(0);
    }
}
