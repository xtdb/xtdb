package core2.vector;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<IVectorWriter<?>> {
    // NOTE: only keeps tracks of rows copied through rowCopier
    IWriterPosition writerPosition();

    IVectorWriter<?> writerForName(String name);
    IVectorWriter<?> writerForName(String name, Object colType);

    IRowCopier rowCopier(IIndirectRelation relation);

    default void clear() {
        for (IVectorWriter<?> writer: this) {
            writer.clear();
        }
    }
}
