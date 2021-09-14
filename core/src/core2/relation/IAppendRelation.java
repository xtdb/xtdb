package core2.relation;

@SuppressWarnings("try")
public interface IAppendRelation extends AutoCloseable, Iterable<IAppendColumn<?>> {
    void appendRelation(IReadRelation sourceRelation);

    IAppendColumn<?> appendColumn(String name);

    IReadRelation read();

    void clear();
}
