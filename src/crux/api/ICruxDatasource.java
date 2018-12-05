package crux.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the database as of a specific business and
 * transaction time.
 */
public interface ICruxDatasource {
    /**
     * Returns the document map for an entity.
     */
    public Map entity(Object eid);

    /**
     * Returns the entity tx for an entity.
     */
    public Map entityTx(Object eid);

    /**
     * Returns a new snapshot for usage with q, allowing for lazy
     * results in a try-with-resources block.
     */
    public Closeable newSnapshot();

    /**
     * Queries the db.
     */
    public Set<List> q(Map query);

    /**
     * Queries the db lazily.
     */
    public List<List> q(Closeable snapshot, Map query);
}
