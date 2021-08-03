package core2.relation;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Date;

public interface IAppendColumn extends AutoCloseable {
    void appendFrom(IReadColumn src, int idx);
    IReadColumn read();

    void appendNull();
    void appendBool(boolean bool);
    void appendDouble(double dbl);
    void appendLong(long lng);
    void appendDateMillis(long date);
    void appendDurationMillis(long millis);
    void appendString(ByteBuffer buf);
    void appendBytes(ByteBuffer buf);
    void appendObject(Object obj);

    @Override
    default void close() {
    }
}
