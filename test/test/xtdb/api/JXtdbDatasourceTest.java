package xtdb.api;

import clojure.lang.Keyword;

import java.time.Duration;
import java.util.*;

import org.junit.*;

import static xtdb.api.TestUtils.*;
import static org.junit.Assert.*;
import static xtdb.api.tx.Transaction.buildTx;

public class JXtdbDatasourceTest {
    private static List<XtdbDocument> documents;
    private static List<TransactionInstant> transactions;

    private static final Keyword pullId1 = Keyword.intern("pull1");
    private static final Keyword pullId2 = Keyword.intern("pull2");

    private static final XtdbDocument pullDocument1 = XtdbDocument.create(pullId1).plus("foo", "foo").plus("bar", 1);
    private static final XtdbDocument pullDocument2 = XtdbDocument.create(pullId2).plus("foo", "bar").plus("bar", 2);

    private static IXtdb node;

    @BeforeClass
    public static void beforeClass() {
        node = IXtdb.startNode();

        ArrayList<XtdbDocument> _documents = new ArrayList<>();
        for (int i=0; i<3; i++) {
            _documents.add(testDocument(i));
        }
        documents = _documents;

        ArrayList<TransactionInstant> _transactions = new ArrayList<TransactionInstant>();

        TestUtils.put(node, pullDocument1, null, null);
        TestUtils.put(node, pullDocument2, null, null);
        sleep(20);
        _transactions.add(put(0, date(-10000), null));
        sleep(20);
        _transactions.add(put(1, date(-8000), date(-7000)));
        sleep(20);
        _transactions.add(delete(date(-9000), date(-8500)));
        sleep(20);
        TransactionInstant last = put(2, date(1000000), null);
        _transactions.add(last);

        node.awaitTx(last, Duration.ofSeconds(10));

        transactions = _transactions;
    }

    @AfterClass
    public static void afterClass() {
        try {
            node.close();
        }
        catch (Exception e) {
            fail();
        }

        node = null;
        documents = null;
        transactions = null;
    }

    @Test
    public void current() {
        checkEntity(node.db(), 0);
        checkEntity(node.openDB(), 0);
        DBBasis basis = new DBBasis(null, null);
        checkEntity(node.db(basis), 0);
        checkEntity(node.openDB(basis), 0);
    }

    @Test
    public void differentValidTimeSameTransactionTime() {
        Date validTime = date(-7500);
        checkEntity(node.db(validTime), 1);
        checkEntity(node.openDB(validTime), 1);
        DBBasis basis = new DBBasis(validTime, null);
        checkEntity(node.db(basis), 1);
        checkEntity(node.openDB(basis), 1);
    }

    @Test
    public void differentValidTimeDifferentTransactionTime() {
        Date validTime = date(-7500);
        Date transactionTime = transactions.get(0).getTime();
        checkEntity(node.db(validTime, transactionTime), 0);
        checkEntity(node.openDB(validTime, transactionTime), 0);
        DBBasis basis = new DBBasis(validTime, TransactionInstant.factory(transactionTime));
        checkEntity(node.db(basis), 0);
        checkEntity(node.openDB(basis), 0);
    }

    @Test
    public void usingJustTransactionId() {
        Date validTime = date(-7500);
        DBBasis basis = new DBBasis(validTime, TransactionInstant.factory(transactions.get(0).getId()));
        checkEntity(node.db(basis), 0);
        checkEntity(node.openDB(basis), 0);

        basis = new DBBasis(validTime, TransactionInstant.factory(transactions.get(1).getId()));
        checkEntity(node.db(basis), 1);
        checkEntity(node.openDB(basis), 1);
    }

    @Test
    public void futureValidTime() {
        Date validTime = date(1001000);

        checkEntity(node.db(validTime), 2);
        checkEntity(node.openDB(validTime), 2);
    }

    @Test
    public void pullTest() {
        IXtdbDatasource db = node.db();
        String projection = "[:xt/id :foo :bar]";

        Map<Keyword, ?> result = db.pull(projection, pullId1);

        assertEquals(pullDocument1.toMap(), result);

        close(db);
    }

    @Test
    public void pullManyIterableTest() {
        IXtdbDatasource db = node.db();
        String projection = "[:xt/id :foo :bar]";

        List<Map<Keyword, ?>> results = db.pullMany(projection, pullId1, pullId2);

        assertEquals(2, results.size());
        assertTrue(results.contains(pullDocument1.toMap()));
        assertTrue(results.contains(pullDocument2.toMap()));

        close(db);
    }

    @Test
    public void pullManyCollectionTest() {
        IXtdbDatasource db = node.db();
        String projection = "[:xt/id :foo :bar]";
        ArrayList<Keyword> ids = new ArrayList<>();
        ids.add(pullId1);
        ids.add(pullId2);

        List<Map<Keyword, ?>> results = db.pullMany(projection, ids);

        assertEquals(2, results.size());
        assertTrue(results.contains(pullDocument1.toMap()));
        assertTrue(results.contains(pullDocument2.toMap()));

        close(db);
    }

    @Test
    public void entityTxTest() {
        IXtdbDatasource db = node.db();
        Map<Keyword,?> tx = db.entityTx(documentId);

        assertHasKeys(tx, DB_ID, CONTENT_HASH, VALID_TIME, TX_TIME, TX_ID);

        long txId = (Long) tx.get(TX_ID);

        TransactionInstant submittedTx = null;
        for (TransactionInstant transaction: transactions) {
            long transactionId = transaction.getId();
            if (txId == transactionId) {
                submittedTx = transaction;
            }
        }

        if (submittedTx == null) {
            fail();
            return;
        }

        assertEquals(submittedTx.getTime(), (Date) tx.get(TX_TIME));
        close(db);
    }

    @Test
    public void queryTest() {
        String query = "{:find [v] :where [[d :version v]]}";
        IXtdbDatasource db = node.db();
        Collection<List<?>> results = db.query(query);

        assertEquals(1, results.size());
        Optional<List<?>> resultRaw = results.stream().findFirst();
        if (resultRaw.isPresent()) {
            List<?> result = resultRaw.get();
            assertEquals(1, result.size());
            long version = (Long) result.get(0);
            assertEquals(0L, version);
        }
        else {
            fail();
        }

        close(db);
    }

    @Test
    public void openQueryTest() {
        String query = "{:find [v] :where [[d :version v]]}";
        IXtdbDatasource db = node.db();
        ICursor<List<?>> results = db.openQuery(query);

        assertTrue(results.hasNext());
        List<?> result = results.next();
        assertFalse(results.hasNext());
        close(results);

        assertEquals(1, result.size());
        long version = (Long) result.get(0);
        assertEquals(0L, version);

        close(db);
    }

    @Test
    public void queryWithArgTest() {
        String query = "{:find [d] :where [[d :version v]] :in [v]}";
        IXtdbDatasource db = node.db();

        Collection<List<?>> results = db.query(query, 0);
        assertEquals(1, results.size());
        Optional<List<?>> resultRaw = results.stream().findFirst();
        if (resultRaw.isPresent()) {
            List<?> result = resultRaw.get();
            assertEquals(1, result.size());
            String id = (String) result.get(0);
            assertEquals(documentId, id);
        }
        else {
            fail();
        }

        results = db.query(query, 1);
        assertEquals(0, results.size());

        close(db);
    }

    @Test
    public void openQueryWithArgTest() {
        String query = "{:find [d] :where [[d :version v]] :in [v]}";
        IXtdbDatasource db = node.db();

        ICursor<List<?>> results = db.openQuery(query, 0);

        assertTrue(results.hasNext());
        List<?> result = results.next();
        assertFalse(results.hasNext());
        close(results);

        assertEquals(1, result.size());
        String id = (String) result.get(0);
        assertEquals(documentId, id);

        results = db.openQuery(query, 1);
        assertFalse(results.hasNext());

        close(results);
        close(db);
    }

    @Test
    public void entityHistoryTest() {
        IXtdbDatasource db = node.db();
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.SortOrder.ASC);
        ICursor<Map<Keyword, ?>> openHistory = db.openEntityHistory(documentId, HistoryOptions.SortOrder.ASC);

        long time = -1;
        for (Map<Keyword, ?> fromHistory: history) {
            assertTrue(openHistory.hasNext());
            Map<Keyword, ?> fromOpenHistory = openHistory.next();

            assertEquals(fromHistory, fromOpenHistory);
            long validTime = ((Date) fromHistory.get(VALID_TIME)).getTime();
            if (time != -1) {
                assertTrue(validTime > time);
            }

            time = validTime;
        }
        assertFalse(openHistory.hasNext());
        assertEquals(5, history.size());

        close(openHistory);
        close(db);
    }

    @Test
    public void validTimeUnspecifiedTest() {
        long lowerBound = (new Date()).getTime();
        IXtdbDatasource db = node.db();
        long upperBound = (new Date()).getTime();
        long validTime = db.validTime().getTime();

        assertTrue(lowerBound <= validTime);
        assertTrue(upperBound >= validTime);

        close(db);
    }

    @Test
    public void validTimeSpecifiedTest() {
        Date validTime = new Date();
        IXtdbDatasource db = node.db(validTime);
        assertEquals(validTime.getTime(), db.validTime().getTime());
        close(db);
    }

    @Test
    public void transactionTimeUnspecifiedTest() {
        IXtdbDatasource db = node.db();
        assertEquals(getLastTransactionTime(), db.transactionTime().getTime());
        close(db);
    }

    @Test
    public void transactionTimeSpecifiedTest() {
        Date transactionTime = date(-50);
        IXtdbDatasource db = node.db(date(-100), transactionTime);
        assertEquals(transactionTime.getTime(), db.transactionTime().getTime());
        close(db);
    }

    @Test
    public void defaultBasisTest() {
        long lowerBound = (new Date()).getTime();
        IXtdbDatasource db = node.db();
        long upperBound = (new Date()).getTime();

        DBBasis basis = db.dbBasis();

        long validTime = basis.getValidTime().getTime();

        assertTrue(lowerBound <= validTime);
        assertTrue(upperBound >= validTime);

        TransactionInstant transactionInstant = basis.getTransactionInstant();

        assertEquals(last(transactions), transactionInstant);

        close(db);
    }

    @Test
    public void customBasisTest() {
        Date validTime = date(-80);
        Date transactionTime = date(90);

        IXtdbDatasource db = node.db(validTime, transactionTime);
        DBBasis basis = db.dbBasis();

        assertEquals(validTime, basis.getValidTime());
        assertEquals(transactionTime, basis.getTransactionInstant().getTime());
    }

    @Test
    public void withTxTest() {
        IXtdbDatasource db = node.db();
        assertNotNull(db.entity(documentId));

        IXtdbDatasource dbWithTx = db.withTx(buildTx(tx -> {
            tx.evict(documentId);
        }));

        assertNull(dbWithTx.entity(documentId));
        //Check we haven't impacted the original snapshot
        assertNotNull(db.entity(documentId));

        close(db);
        close(dbWithTx);
    }

    /*
    Utils
     */
    private long getLastTransactionTime() {
        return last(transactions).getTime().getTime();
    }

    private void checkEntity(IXtdbDatasource db, int documentIndex) {
        XtdbDocument document = db.entity(documentId);
        if (documentIndex >= 0) {
            assertEquals(documents.get(documentIndex), document);
        }
        else {
            assertNull(document);
        }
        close(db);
    }

    private static TransactionInstant delete(Date validTime, Date endValidTime) {
        return TestUtils.delete(node, documentId, validTime, endValidTime);
    }

    private static TransactionInstant put(int documentIndex, Date validTime, Date endValidTime) {
        XtdbDocument document = documents.get(documentIndex);
        return TestUtils.put(node, document, validTime, endValidTime);
    }
}
