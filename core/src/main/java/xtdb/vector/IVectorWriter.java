package xtdb.vector;

import org.apache.arrow.vector.ValueVector;

public interface IVectorWriter extends IValueWriter, AutoCloseable {

    /**
     * Maintains the next position to be written to.
     *
     * Automatically incremented by the various `write` methods, and any {@link IVectorWriter#rowCopier}s.
     */
    IWriterPosition writerPosition();

    /**
     * Returns the underlying vector.
     *
     * This method calls {@link ValueVector#setValueCount} on the underlying vector, so that all of the values written
     * become visible through the Arrow Java API - we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    ValueVector getVector();

    IRowCopier rowCopier(ValueVector srcVector);

    @Override
    IVectorWriter structKeyWriter(String key);

    @Override
    IVectorWriter writerForType(Object colType);

    @Override
    IVectorWriter writerForTypeId(byte typeId);

    void clear();

    @Override
    default void close() {
        getVector().close();
    }
}
