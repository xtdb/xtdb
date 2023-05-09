package xtdb.vector;

@SuppressWarnings("try")
public interface IRelationWriter extends AutoCloseable, Iterable<IVectorWriter> {

    /**
     * Maintains the next position to be written to.
     *
     * This is incremented either by using the {@link IRelationWriter#rowCopier}, or by explicitly calling {@link IRelationWriter#endRow()}
     */
    IWriterPosition writerPosition();

    void endRow();

    IVectorWriter writerForName(String name);
    IVectorWriter writerForName(String name, Object colType);

    IRowCopier rowCopier(IIndirectRelation relation);

    void clear();
}
