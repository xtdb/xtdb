package core2.relation;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<IColumnWriter<?>> {
    IColumnWriter<?> columnWriter(String name);
}
