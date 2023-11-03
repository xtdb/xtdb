package xtdb;

@SuppressWarnings("try")
public interface IArrowWriter extends AutoCloseable {
    void writeBatch();
    void end();
}
