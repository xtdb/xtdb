package core2.relation;

public interface IReadRelation extends AutoCloseable {

    Iterable<IReadColumn> readColumns();
    IReadColumn readColumn(String colName);
    int rowCount();

    @Override
    default void close() {
    }
}
