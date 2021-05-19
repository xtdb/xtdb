package core2.relation;

public interface IAppendRelation extends AutoCloseable {
    IAppendColumn appendColumn(String colName);
    IReadRelation read();

    @Override
    default void close() {
    }
}
