package core2.vector;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<IVectorWriter<?>> {
    IVectorWriter<?> writerForName(String name);
}
