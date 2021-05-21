package core2.relation;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;

import java.nio.ByteBuffer;
import java.util.EnumSet;

public interface IReadColumn extends AutoCloseable {
    String getName();
    IReadColumn rename(String colName);
    int valueCount();
    EnumSet<MinorType> minorTypes();

    boolean getBool(int idx);
    long getLong(int idx);
    long getDateMillis(int idx);
    long getDurationMillis(int idx);
    double getDouble(int idx);
    ByteBuffer getBuffer(int idx);
    Object getObject(int idx);

    ValueVector _getInternalVector(int idx);
    int _getInternalIndex(int idx);

    @Override
    default void close() {
    }
}
