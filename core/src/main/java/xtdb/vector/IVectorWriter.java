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

    default void writeValue(IValueReader valueReader) {
        if (valueReader.isNull()) writeNull();
        else writeValue0(valueReader);
    }

    /**
     * @param valueReader a non-null IValueReader
     */
    void writeValue0(IValueReader valueReader);

    IVectorWriter structKeyWriter(String key);

    IVectorWriter structKeyWriter(String key, FieldType fieldType);

    void startStruct();

    void endStruct();

    IVectorWriter listElementWriter();

    IVectorWriter listElementWriter(FieldType fieldType);

    void startList();

    void endList();

    @Override
    IVectorWriter legWriter(Keyword leg);

    IVectorWriter legWriter(ArrowType leg);

    IVectorWriter legWriter(Keyword leg, FieldType fieldType);

    void clear();

    @Override
    default void close() {
        getVector().close();
    }
}
