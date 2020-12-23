package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import crux.api.document.CruxDocument;
import crux.api.document.ICruxDocument;
import crux.api.transaction.Transaction;
import crux.api.transaction.TransactionWrapper;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

public class TransactionTest {
    private static class PersonDocument implements ICruxDocument {
        private final String id;
        private final String name;
        private final String lastName;
        private final int version;

        private PersonDocument(String id, String name, String lastName, int version) {
            this.id = id;
            this.name = name;
            this.lastName = lastName;
            this.version = version;
        }

        @Override
        public Object getDocumentId() {
            return id;
        }

        @Override
        public Map<String, Object> getDocumentContents() {
            HashMap<String, Object> ret = new HashMap<>();
            ret.put("person/name", name);
            ret.put("person/lastName", lastName);
            ret.put("person/version", version);
            return ret;
        }
    }

    private static final String pabloId = "PabloPicasso";
    private static List<Date> times;
    private static List<PersonDocument> pablos;
    private static ICruxAPI node;

    @BeforeClass
    public static void setup() {
        node = Crux.startNode();
        ArrayList<Date> times = new ArrayList<>();
        ArrayList<PersonDocument> pablos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //The few hours after Y2K
            long seconds = 946684800 + i * 3600;
            Date time = Date.from(Instant.ofEpochSecond(seconds));
            times.add(time);

            PersonDocument pablo = new PersonDocument(pabloId, "Pablo", "Picasso", i);
            pablos.add(pablo);
        }
        TransactionTest.times = times;
        TransactionTest.pablos = pablos;
    }

    @After
    public void cleanUp() {
        //Just restart. Makes everything clean.
        node = Crux.startNode();
        sync();
    }

    @AfterClass
    public static void closeNode() {
        try {
            node.close();
        }
        catch (Exception e) {
            System.out.println("Exception in node closure");
        }
    }

    
    private Map<Keyword,?> submitTx(boolean shouldAbort, Consumer<Transaction.Builder> f) {
        Transaction transaction = Transaction.build(f);
        return submitTx(shouldAbort, transaction);
    }

    /**
     * This will also check that we can successfully rebuild the Transaction from the TxLog
     * (assuming the transaction is supposed to go through)
     */
    @SuppressWarnings("unchecked")
    private Map<Keyword,?> submitTx(boolean shouldAbort, Transaction transaction) {
        Map<Keyword,?> submitted = node.submit(transaction);

        sync();

        long txId = (long) submitted.get(Keyword.intern("crux.tx/tx-id"));
        ICursor<TransactionWrapper> cursor = node.openTxLog(txId - 1);
        if (shouldAbort) {
            Assert.assertFalse(cursor.hasNext());
            try {
                cursor.close();
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
            return submitted;
        }

        Assert.assertTrue(cursor.hasNext());
        TransactionWrapper transactionWrapper = cursor.next();
        Assert.assertFalse(cursor.hasNext());

        try {
            cursor.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertNotNull(transactionWrapper);
        Transaction transactionFromLog = transactionWrapper.getTransaction();
        Assert.assertNotNull(transactionFromLog);
        Assert.assertEquals(txId, transactionWrapper.getId());
        Assert.assertEquals(transaction, transactionFromLog);

        return submitted;
    }

    private void sync() {
        node.sync(Duration.ofSeconds(10));
    }

    private IPersistentMap pabloEdn(int version) {
        return pablos.get(version).toEdn();
    }

    private void assertPabloVersion(int version) {
        assertPabloVersion(version, null);
    }

    private void assertPabloVersion(int version, int timeIndex) {
        assertPabloVersion(version, time(timeIndex));
    }

    private void assertPabloVersion(int version, Date validTime) {
        Map<Keyword, Object> fromDb;
        if (validTime == null) {
            fromDb = node.db().entity(pabloId);
        }
        else {
            fromDb = node.db(validTime).entity(pabloId);
        }

        if (fromDb == null) {
            Assert.fail();
        }
        
        CruxDocument document = CruxDocument.factory(fromDb);
        Assert.assertEquals(pabloEdn(version), document.toEdn());
    }

    private void assertNoPablo() {
        assertNoPablo(null);
    }

    private void assertNoPablo(int timeIndex) {
        assertNoPablo(time(timeIndex));
    }

    private void assertNoPablo(Date validTime) {
        Object result;
        if (validTime == null) {
            result = node.db().entity(pabloId);
        }
        else {
            result = node.db(validTime).entity(pabloId);
        }

        Assert.assertNull(result);
    }
    
    private PersonDocument pablo(int version) {
        return pablos.get(version);
    }
    
    private Date time(int timeIndex) {
        return times.get(timeIndex);
    }

    @Test
    public void putNow() {
        submitTx(false, tx -> {
            tx.put(pablos.get(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void putAtTime() {
        submitTx(false, tx -> {
            tx.put(pablo(0), time(1));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0);
    }

    @Test
    public void putWithEndValidTime() {
        submitTx(false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void putDifferentVersions() {
        submitTx (false,  tx -> {
            tx.put(pablo(0), time(1));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void deleteNow() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId);
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo();
    }

    @Test
    public void deleteAtSpecificTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId, time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void deleteWithEndTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId, time(3), time(5));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertPabloVersion(0, 5);
        assertPabloVersion(0, 6);
        assertPabloVersion(0);
    }

    @Test
    public void deleteWithSubsequentChange() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.put(pablo(1), time(5));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0, 3);
        assertPabloVersion(0, 4);
        assertPabloVersion(1, 5);
        assertPabloVersion(1, 6);
        assertPabloVersion(1);

        submitTx(false, tx -> {
            tx.delete(pabloId, time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertPabloVersion(1, 5);
        assertPabloVersion(1, 6);
        assertPabloVersion(1);
    }

    @Test
    public void successfulMatchNow() {
        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx(false, tx -> {
            tx.match(pablo(0));
            tx.put(pablo(1));
        });

        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulMatchNow() {
        submitTx (false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx (true, tx -> {
            tx.match(pablo(2));
            tx.put(pablo(3));
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulMatchWithValidTime() {
        submitTx (false, tx -> {
           tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (false, tx -> {
            tx.match(pablo(0), time(2));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulMatchWithValidTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (true, tx -> {
            tx.match(pablo(0), time(4));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void successfulEmptyMatch() {
        assertNoPablo();

        submitTx (false,  tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void unsuccessfulEmptyMatch() {
        submitTx (false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx (true, tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulEmptyMatchAtTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (false, tx -> {
            tx.matchNotExists(pabloId, time(3));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulEmptyMatchAtTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (true, tx -> {
            tx.matchNotExists(pabloId, time(2));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void reusingTransactions() {
        Transaction transaction = Transaction.build( tx -> {
            tx.delete(pabloId);
        });

        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx(false, transaction);

        assertNoPablo();

        submitTx(false, tx -> {
            tx.put(pablo(1));
        });

        assertPabloVersion(1);

        submitTx(false, transaction);

        assertNoPablo();
    }

    @Test
    public void usingTransactionsFromLog() {
        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        Map<Keyword, ?> txData = submitTx(false, tx -> {
            tx.delete(pabloId);
        });

        assertNoPablo();

        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        long txId = (long) txData.get(Keyword.intern("crux.tx/tx-id"));
        ICursor<TransactionWrapper> entireHistory = node.openTxLog(null);
        Transaction transaction = null;
        while (entireHistory.hasNext()) {
            TransactionWrapper wrapper = entireHistory.next();
            if (wrapper.getId() == txId) {
                transaction = wrapper.getTransaction();
            }
        }

        Assert.assertNotNull(transaction);

        submitTx(false, transaction);

        assertNoPablo();
    }
}