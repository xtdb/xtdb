package xtdb.vector;

import clojure.lang.Keyword;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public interface IVectorWriter extends IValueWriter, AutoCloseable {

    /**
     * <p> Maintains the next position to be written to. </p>
     *
     * <p> Automatically incremented by the various `write` methods, and any {@link IVectorWriter#rowCopier}s. </p>
     */
    IVectorPosition writerPosition();

    ValueVector getVector();

    Field getField();

    /**
     * This method calls {@link ValueVector#setValueCount} on the underlying vector, so that all of the values written
     * become visible through the Arrow Java API - we don't call this after every write because (for composite vectors, and especially unions)
     * it's not the cheapest call.
     */
    default void syncValueCount() {
        getVector().setValueCount(writerPosition().getPosition());
    }

    IRowCopier rowCopier(ValueVector srcVector);

    /**
     * won't create
     */
    @Override
    IVectorWriter structKeyWriter(String key);

    IVectorWriter structKeyWriter(String key, FieldType fieldType);

    @Override
    IVectorWriter listElementWriter();

    IVectorWriter listElementWriter(FieldType fieldType);

    @Override
    IVectorWriter legWriter(Keyword leg);

    IVectorWriter legWriter(ArrowType leg);

    IVectorWriter legWriter(Keyword leg, FieldType fieldType);

    @Override
    @Deprecated
    IVectorWriter writerForTypeId(byte typeId);

    void clear();

    @Override
    default void close() {
        getVector().close();
    }
}
