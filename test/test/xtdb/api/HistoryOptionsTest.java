package xtdb.api;

import clojure.lang.Keyword;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.junit.*;

import static xtdb.api.TestUtils.*;
import static org.junit.Assert.*;

public class HistoryOptionsTest {
    private static List<CruxDocument> documents;
    private static List<TransactionInstant> transactions;
    private static ICruxAPI node;

    private ICruxDatasource db;

    @BeforeClass
    public static void beforeClass() {
        node = Crux.startNode();

        ArrayList<CruxDocument> _documents = new ArrayList<>();
        for (int i=0; i<5; i++) {
            _documents.add(testDocument(i));
        }
        documents = _documents;

        ArrayList<TransactionInstant> _transactions = new ArrayList<>();

        sleep(20);
        _transactions.add(p(0, date(0), null));
        sleep(20);
        _transactions.add(p(1, date(-200), null));
        sleep(20);
        _transactions.add(p(3, date(-100), date(50)));
        sleep(20);
        _transactions.add(d(date(-50), null));
        sleep(20);
        _transactions.add(p(2, date(50), null));
        sleep(20);
        TransactionInstant last = d(date(-100), date(-75));
        _transactions.add(last);

        node.awaitTx(last, Duration.ofSeconds(10));

        transactions = _transactions;
    }

    @AfterClass
    public static void afterClass() {
        close(node);
        node = null;
        documents = null;
        transactions = null;
    }

    @Before
    public void before() {
        db = node.db();
    }

    @After
    public void after() {
        try {
            db.close();
        }
        catch (Exception e) {
            fail();
        }
        db = null;
    }

    @Test
    public void ascending() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.SortOrder.ASC);

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);

        List<Long> times = history.stream().map(keywordMap -> ((Date) keywordMap.get(VALID_TIME)).getTime()).collect(Collectors.toList());
        List<Long> compare = times.stream().sorted().collect(Collectors.toList());
        assertEquals(compare, times);
        assertEquals(6, history.size());
    }

    @Test
    public void descending() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.SortOrder.DESC);

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);

        List<Long> times = history.stream().map(keywordMap -> ((Date) keywordMap.get(VALID_TIME)).getTime()).collect(Collectors.toList());
        List<Long> compare = times.stream().sorted().collect(Collectors.toList());
        Collections.reverse(compare);
        assertEquals(compare, times);
        assertEquals(6, history.size());
    }

    @Test
    public void withCorrections() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).withCorrections(true));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(10, history.size());
    }

    @SuppressWarnings("ConstantConditions")
    @Test(timeout = 1000)
    public void withDocs() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).withDocs(true));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH, DOC);
        assertAccurate(history);

        for (Map<Keyword, ?> entry: history) {
            Object contentHash = entry.get(CONTENT_HASH);
            Object document = entry.get(DOC);
            if (contentHash.equals(null)) {
                assertNull(document);
            }
            else {
                assertNotNull(document);
            }
        }
    }

    @Test
    public void startValidTime() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).startValidTime(date(-50)));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(3, history.size());
    }

    @Test
    public void startTransaction() {
        TransactionInstant tx = transactions.get(2);
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).startTransaction(tx));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(5, history.size());
    }

    @Test
    public void startTransactionTime() {
        TransactionInstant tx = transactions.get(2);
        Date txTime = tx.getTime();
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).startTransactionTime(txTime));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(5, history.size());
    }

    @Test
    public void endValidTime() {
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).endValidTime(date(-50)));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(3, history.size());
    }

    @Test
    public void endTransaction() {
        TransactionInstant tx = transactions.get(2);
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).endTransaction(tx));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(3, history.size());
    }

    @Test
    public void endTransactionTime() {
        TransactionInstant tx = transactions.get(2);
        Date txTime = tx.getTime();
        List<Map<Keyword, ?>> history = db.entityHistory(documentId, HistoryOptions.create(HistoryOptions.SortOrder.ASC).endTransactionTime(txTime));

        assertHasKeys(history, TX_TIME, TX_ID, VALID_TIME, CONTENT_HASH);
        assertAccurate(history);
        assertEquals(3, history.size());
    }

    /*
    Utils
     */
    private static void assertAccurate(List<Map<Keyword, ?>> maps) {
        for (Map<Keyword, ?> map: maps) {
            Date txTime = (Date) map.get(TX_TIME);
            long txId = (Long) map.get(TX_ID);

            TransactionInstant tx = transactions.get((int) txId);
            assertEquals((long) tx.getId(), txId);
            assertEquals(tx.getTime(), txTime);
        }
    }

    private static TransactionInstant d(Date validTime, Date endValidTime) {
        return delete(node, documentId, validTime, endValidTime);
    }

    private static TransactionInstant p(int documentIndex, Date validTime, Date endValidTime) {
        CruxDocument document = documents.get(documentIndex);
        return put(node, document, validTime, endValidTime);
    }
}
