package xtdb.vector;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

public interface IVectorWriter extends IValueWriter, AutoCloseable {

    /**
     * <p> Maintains the next position to be written to. </p>
     *
     * <p> Automatically incremented by the various `write` methods, and any {@link IVectorWriter#rowCopier}s. </p>
     */
    IWriterPosition writerPosition();

    ValueVector getVector();

    /**
     * This method calls {@link ValueVector#setValueCount} on the underlying vector, so that all of the values written
     * become visible through the Arrow Java API - we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    default void syncValueCount() {
        getVector().setValueCount(writerPosition().getPosition());
    }

    IRowCopier rowCopier(ValueVector srcVector);

    @Override
    IVectorWriter structKeyWriter(String key);

    @Override
    IVectorWriter structKeyWriter(String key, Object colType);

    @Override
    IVectorWriter writerForType(Object colType);

    @Override
    IVectorWriter writerForTypeId(byte typeId);

    IVectorWriter writerForField(Field field);

    @Override
    IVectorWriter listElementWriter();

    void clear();

    @Override
    default void close() {
        getVector().close();
    }
}
