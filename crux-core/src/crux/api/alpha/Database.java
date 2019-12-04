package crux.api.alpha;

import clojure.lang.APersistentMap;
import clojure.lang.LazySeq;
import clojure.lang.Symbol;
import crux.api.ICruxAPI;
import crux.api.ICruxDatasource;
import clojure.lang.Keyword;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;

import static crux.api.alpha.ResultTuple.resultTuple;

public class Database {
    private final ICruxDatasource db;

    private Database(ICruxDatasource db) {
        this.db = db;
    }

    protected static Database database(ICruxAPI node) {
        return new Database(node.db());
    }

    protected static Database database(ICruxAPI node, Date validTime) {
        return new Database(node.db(validTime));
    }

    protected static Database database(ICruxAPI node, Date validTime, Date transactionTime) {
        return new Database(node.db(validTime, transactionTime));
    }

    /**
     * Returns a new snapshot allowing for lazy query results in a
     * try-with-resources block using {@link #query(Closeable snapshot,
     * Query query)}. Can also be used for {@link
     * #historyAscending(Closeable snapshot, CruxId eid)} and {@link
     * #historyDescending(Closeable snapshot, CruxId eid)}
     *
     * @return an implementation specific snapshot
     */
    public Closeable newSnapshot() {
        return db.newSnapshot();
    }


    /**
     * Submits a Query to the database, and returns a list of results from the query
     * @param query Query to perform on the Database
     * @return List of ResultTuple objects representing the results from the query
     * @see ResultTuple
     */
    public List<ResultTuple> query(Query query) {
        Collection<List<?>> queryResult = db.q(query.toEdn());
        List<Symbol> symbols = query.findSymbols();
        return queryResult.stream().map(tuple -> resultTuple(symbols, tuple)).collect(Collectors.toList());
    }

    public Collection<List<?>> query(String query) {
        return db.q(query);
    }

    @SuppressWarnings("unchecked")
    /**
     * Queries the db lazily.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param query    the query in map, vector or string form.
     * @return         a lazy sequence of result tuples.
     */
    public Iterator<ResultTuple> query(Closeable snapshot, Query query) {
        LazySeq lazyQueryResult = (LazySeq) db.q(snapshot, query.toEdn());
        List<Symbol> symbols = query.findSymbols();
        return lazyQueryResult.stream()
            .map(tuple -> resultTuple(symbols, (List<?>) tuple))
            .iterator();
    }

    public Iterable<List<?>> query(Closeable snapshot, String query) {
        return db.q(snapshot, query);
    }

    /**
     * Retrieves a Document for an entity in the Database
     * @param id Id of entity to retrieve
     * @return Document representing the entity
     */
    public Document entity(CruxId id) {
        Map<Keyword, Object> entityDoc = db.entity(id.toEdn());
        if(entityDoc != null) {
            return Document.document(entityDoc);
        } else {
            return null;
        }

    }

    /**
     * Retrieves an EntityTx for an entity in the Database
     * @param id Id of entity to retrieve
     * @return EntityTx information for the entity
     * @see EntityTx
     */
    public EntityTx entityTx(CruxId id) {
        Map<Keyword, ?> entityTx = db.entityTx(id.toEdn());
        if(entityTx != null) {
            return EntityTx.entityTx(entityTx);
        } else {
            return null;
        }

    }

    @SuppressWarnings("unchecked")
    /**
     * Retrieves entity history lazily in chronological order from and
     * including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param eid      an object that can be coerced into an entity id.
     * @return         a lazy sequence of history.
     */
    public Iterator<EntityTxWithDocument> historyAscending(Closeable snapshot, CruxId eid) {
        LazySeq historyAscending = (LazySeq) db.historyAscending(snapshot, eid.toEdn());
        // Maps returned from lazy history functions are APersistentMaps - convert to HashMaps for easier processing
        return historyAscending.stream()
            .map(entityTxWithDoc ->
                EntityTxWithDocument.entityTxWithDocument(new HashMap<Keyword, Object>((APersistentMap) entityTxWithDoc)))
            .iterator();
    }

    @SuppressWarnings("unchecked")
    /**
     * Retrieves entity history lazily in reverse chronological order
     * from and including the valid time of the db while respecting
     * transaction time. Includes the documents.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()}.
     * @param eid      an object that can be coerced into an entity id.
     * @return         a lazy sequence of history.
     */
    public Iterator<EntityTxWithDocument> historyDescending(Closeable snapshot, CruxId eid) {
        LazySeq historyAscending = (LazySeq) db.historyDescending(snapshot, eid.toEdn());
        // Maps returned from lazy history functions are APersistentMaps - convert to HashMaps for easier processing
        return historyAscending.stream()
            .map(entityTxWithDoc ->
                EntityTxWithDocument.entityTxWithDocument(new HashMap<Keyword, Object>((APersistentMap) entityTxWithDoc)))
            .iterator();
    }

    /**
     * The valid time of this db.
     * If valid time wasn't specified at the moment of the db value retrieval
     * then valid time will be time of the latest transaction.
     *
     * @return the valid time of this db.
     */
    public Date validTime() {
        return db.validTime();
    }

    /**
     * @return the time of the latest transaction applied to this db value.
     * If a tx time was specified when db value was acquired then returns
     * the specified time.
     */
    public Date transactionTime() {
        return db.transactionTime();
    }
}
