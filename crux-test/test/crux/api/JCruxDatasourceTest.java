package crux.api;

import clojure.lang.Keyword;

import org.junit.*;

import java.time.Duration;
import java.util.*;

import static crux.api.TestUtils.*;

public class JCruxDatasourceTest {
    private static List<TestDocument> documents;
    private static List<TransactionInstant> transactions;

    private static final Keyword projectId1 = Keyword.intern("project1");
    private static final Keyword projectId2 = Keyword.intern("project2");
    private static final Keyword fooId = Keyword.intern("foo");
    private static final Keyword barId = Keyword.intern("bar");

    private static final AbstractCruxDocument projectDocument1 = new AbstractCruxDocument() {
        @Override
        public Object getId() {
            return projectId1;
        }

        @Override
        public Map<Keyword, Object> getData() {
            HashMap<Keyword, Object> ret = new HashMap<>();
            ret.put(fooId, "foo");
            ret.put(barId, 1);
            return ret;
        }
    };

    private static final AbstractCruxDocument projectDocument2 = new AbstractCruxDocument() {
        @Override
        public Object getId() {
            return projectId2;
        }

        @Override
        public Map<Keyword, Object> getData() {
            HashMap<Keyword, Object> ret = new HashMap<>();
            ret.put(fooId, "bar");
            ret.put(barId, 2);
            return ret;
        }
    };

    private static ICruxAPI node;

    @BeforeClass
    public static void beforeClass() {
        node = Crux.startNode();

        ArrayList<TestDocument> _documents = new ArrayList<>();
        for (int i=0; i<3; i++) {
            _documents.add(new TestDocument(i));
        }
        documents = _documents;

        ArrayList<TransactionInstant> _transactions = new ArrayList<TransactionInstant>();

        put(node, projectDocument1, null, null);
        put(node, projectDocument2, null, null);
        sleep(20);
        _transactions.add(p(0, date(-10000), null));
        sleep(20);
        _transactions.add(p(1, date(-8000), date(-7000)));
        sleep(20);
        _transactions.add(d(date(-9000), date(-8500)));
        sleep(20);
        TransactionInstant last = p(2, date(1000000), null);
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
            Assert.fail();
        }

        node = null;
        documents = null;
        transactions = null;
    }

    @Test
    public void current() {
        checkEntity(node.db(), 0);
        checkEntity(node.openDB(), 0);
        HashMap<Keyword, Object> basis = new HashMap<>();
        checkEntity(node.db(basis), 0);
        checkEntity(node.openDB(basis), 0);
    }

    @Test
    public void differentValidTimeSameTransactionTime() {
        Date validTime = date(-7500);
        checkEntity(node.db(validTime), 1);
        checkEntity(node.openDB(validTime), 1);
        HashMap<Keyword, Object> basis = new HashMap<>();
        basis.put(VALID_TIME, validTime);
        checkEntity(node.db(basis), 1);
        checkEntity(node.openDB(basis), 1);
    }

    @Test
    public void differentValidTimeDifferentTransactionTime() {
        Date validTime = date(-7500);
        Date transactionTime = transactions.get(0).getTime();
        checkEntity(node.db(validTime, transactionTime), 0);
        checkEntity(node.openDB(validTime, transactionTime), 0);
        HashMap<Keyword, Object> basis = new HashMap<>();
        basis.put(VALID_TIME, validTime);
        basis.put(TX_TIME, transactionTime);
        checkEntity(node.db(basis), 0);
        checkEntity(node.openDB(basis), 0);
    }

    @Test
    public void futureValidTime() {
        Date validTime = date(1001000);

        checkEntity(node.db(validTime), 2);
        checkEntity(node.openDB(validTime), 2);
    }

    @Test
    public void projectTest() {
        ICruxDatasource db = node.db();
        String projection = "[:crux.db/id :foo :bar]";

        Map<Keyword, ?> result = db.project(projection, projectId1);

        Assert.assertEquals(projectDocument1.toMap(), result);

        close(db);
    }

    @Test
    public void projectManyIterableTest() {
        ICruxDatasource db = node.db();
        String projection = "[:crux.db/id :foo :bar]";

        List<Map<Keyword, ?>> results = db.projectMany(projection, projectId1, projectId2);

        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(projectDocument1.toMap()));
        Assert.assertTrue(results.contains(projectDocument2.toMap()));

        close(db);
    }

    @Test
    public void projectManyCollectionTest() {
        ICruxDatasource db = node.db();
        String projection = "[:crux.db/id :foo :bar]";
        ArrayList<Keyword> ids = new ArrayList<>();
        ids.add(projectId1);
        ids.add(projectId2);

        List<Map<Keyword, ?>> results = db.projectMany(projection, ids);

        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(projectDocument1.toMap()));
        Assert.assertTrue(results.contains(projectDocument2.toMap()));

        close(db);
    }

    @Test
    public void entityTxTest() {
        ICruxDatasource db = node.db();
        Map<Keyword,?> tx = db.entityTx(TestDocument.documentId);

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
            Assert.fail();
            return;
        }

        Assert.assertEquals(submittedTx.getTime(), (Date) tx.get(TX_TIME));
        close(db);
    }

    @Test
    public void queryTest() {
        String query = "{:find [v] :where [[d :version v]]}";
        ICruxDatasource db = node.db();
        Collection<List<?>> results = db.query(query);

        Assert.assertEquals(1, results.size());
        Optional<List<?>> resultRaw = results.stream().findFirst();
        if (resultRaw.isPresent()) {
            List<?> result = resultRaw.get();
            Assert.assertEquals(1, result.size());
            long version = (Long) result.get(0);
            Assert.assertEquals(0L, version);
        }
        else {
            Assert.fail();
        }

        close(db);
    }

    @Test
    public void openQueryTest() {
        String query = "{:find [v] :where [[d :version v]]}";
        ICruxDatasource db = node.db();
        ICursor<List<?>> results = db.openQuery(query);

        Assert.assertTrue(results.hasNext());
        List<?> result = results.next();
        Assert.assertFalse(results.hasNext());
        close(results);

        Assert.assertEquals(1, result.size());
        long version = (Long) result.get(0);
        Assert.assertEquals(0L, version);

        close(db);
    }

    @Test
    public void queryWithArgTest() {
        String query = "{:find [d] :where [[d :version v]] :in [v]}";
        ICruxDatasource db = node.db();

        Collection<List<?>> results = db.query(query, 0);
        Assert.assertEquals(1, results.size());
        Optional<List<?>> resultRaw = results.stream().findFirst();
        if (resultRaw.isPresent()) {
            List<?> result = resultRaw.get();
            Assert.assertEquals(1, result.size());
            Keyword id = (Keyword) result.get(0);
            Assert.assertEquals(TestDocument.documentId, id);
        }
        else {
            Assert.fail();
        }

        results = db.query(query, 1);
        Assert.assertEquals(0, results.size());

        close(db);
    }

    @Test
    public void openQueryWithArgTest() {
        String query = "{:find [d] :where [[d :version v]] :in [v]}";
        ICruxDatasource db = node.db();

        ICursor<List<?>> results = db.openQuery(query, 0);

        Assert.assertTrue(results.hasNext());
        List<?> result = results.next();
        Assert.assertFalse(results.hasNext());
        close(results);

        Assert.assertEquals(1, result.size());
        Keyword id = (Keyword) result.get(0);
        Assert.assertEquals(TestDocument.documentId, id);

        results = db.openQuery(query, 1);
        Assert.assertFalse(results.hasNext());

        close(results);
        close(db);
    }

    @Test
    public void entityHistoryTest() {
        ICruxDatasource db = node.db();
        List<Map<Keyword, ?>> history = db.entityHistory(TestDocument.documentId, HistoryOptions.SortOrder.ASC);
        ICursor<Map<Keyword, ?>> openHistory = db.openEntityHistory(TestDocument.documentId, HistoryOptions.SortOrder.ASC);

        long time = -1;
        for (Map<Keyword, ?> fromHistory: history) {
            Assert.assertTrue(openHistory.hasNext());
            Map<Keyword, ?> fromOpenHistory = openHistory.next();

            Assert.assertEquals(fromHistory, fromOpenHistory);
            long validTime = ((Date) fromHistory.get(VALID_TIME)).getTime();
            if (time != -1) {
                Assert.assertTrue(validTime > time);
            }

            time = validTime;
        }
        Assert.assertFalse(openHistory.hasNext());
        Assert.assertEquals(5, history.size());

        close(openHistory);
        close(db);
    }

    @Test
    public void validTimeUnspecifiedTest() {
        long lowerBound = (new Date()).getTime();
        ICruxDatasource db = node.db();
        long upperBound = (new Date()).getTime();
        long validTime = db.validTime().getTime();

        Assert.assertTrue(lowerBound <= validTime);
        Assert.assertTrue(upperBound >= validTime);

        close(db);
    }

    @Test
    public void validTimeSpecifiedTest() {
        Date validTime = new Date();
        ICruxDatasource db = node.db(validTime);
        Assert.assertEquals(validTime.getTime(), db.validTime().getTime());
        close(db);
    }

    @Test
    public void transactionTimeUnspecifiedTest() {
        ICruxDatasource db = node.db();
        Assert.assertEquals(getLastTransactionTime(), db.transactionTime().getTime());
        close(db);
    }

    @Test
    public void transactionTimeSpecifiedTest() {
        Date transactionTime = date(-50);
        ICruxDatasource db = node.db(date(-100), transactionTime);
        Assert.assertEquals(transactionTime.getTime(), db.transactionTime().getTime());
        close(db);
    }

    @Test
    public void basisTest() {
        long lowerBound = (new Date()).getTime();
        ICruxDatasource db = node.db();
        long upperBound = (new Date()).getTime();

        long lastTransactionTime = getLastTransactionTime();
        Map<Keyword, ?> basis = db.dbBasis();

        assertHasKeys(basis, VALID_TIME, TX);

        long validTime = ((Date) basis.get(VALID_TIME)).getTime();

        Assert.assertTrue(lowerBound <= validTime);
        Assert.assertTrue(upperBound >= validTime);

        @SuppressWarnings("unchecked")
        Map<Keyword, ?> tx = (Map<Keyword, ?>) basis.get(TX);

        assertHasKeys(tx, TX_TIME, TX_ID);

        long transactionTime = ((Date) tx.get(TX_TIME)).getTime();
        Assert.assertEquals(getLastTransactionTime(), transactionTime);

        long txId = (Long) tx.get(TX_ID);
        long lastTxId = last(transactions).getId();

        Assert.assertEquals(lastTxId, txId);

        close(db);
    }

    /*
    Utils
     */
    private long getLastTransactionTime() {
        return last(transactions).getTime().getTime();
    }

    private void checkEntity(ICruxDatasource db, int documentIndex) {
        AbstractCruxDocument document = db.entity(TestDocument.documentId);
        if (documentIndex >= 0) {
            Assert.assertEquals(documents.get(documentIndex), document);
        }
        else {
            Assert.assertNull(document);
        }
        close(db);
    }

    private static TransactionInstant d(Date validTime, Date endValidTime) {
        return delete(node, TestDocument.documentId, validTime, endValidTime);
    }

    private static TransactionInstant p(int documentIndex, Date validTime, Date endValidTime) {
        AbstractCruxDocument document = documents.get(documentIndex);
        return put(node, document, validTime, endValidTime);
    }
}
