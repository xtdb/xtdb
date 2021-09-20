package core2.relation;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<IColumnWriter<?>> {
    void appendRelation(IRelationReader sourceRelation);

    IColumnWriter<?> columnWriter(String name);

    IRelationReader read();

    void clear();
}
