package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import crux.api.document.CruxDocument;
import crux.api.document.ICruxDocument;
import crux.api.transaction.Transaction;
import org.junit.*;

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
    private static ICruxAPI node;

    @BeforeClass
    public static void setup() {
        node = Crux.startNode();
        ArrayList<Date> times = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //The few hours after Y2K
            long seconds = 946684800 + i * 3600;
            Date time = Date.from(Instant.ofEpochSecond(seconds));
            times.add(time);
        }
        TransactionTest.times = times;
    }

    @After
    public void cleanUp() {
        submitTx( tx -> {
            tx.evict(pabloId);
        });

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

    @SuppressWarnings("unchecked")
    private Map<Keyword,?> submitTx(Consumer<Transaction.Builder> f) {
        Transaction transaction = Transaction.build(f);
        Map<Keyword,?> ret = node.submitTx(transaction.toEdn());
        sync();
        return ret;
    }

    private void sync() {
        node.sync(Duration.ofSeconds(10));
    }

    private PersonDocument createPablo(int version) {
        return new PersonDocument(pabloId, "Pablo", "Picasso", version);
    }

    private IPersistentMap pabloEdn(int version) {
        return createPablo(version).toEdn();
    }

    private void assertPabloVersion(int version) {
        assertPabloVersion(version, null);
    }

    private void assertPabloVersion(int version, int timeIndex) {
        assertPabloVersion(version, times.get(timeIndex));
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
        assertNoPablo(times.get(timeIndex));
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

    @Test
    public void putNow() {
        PersonDocument pablo = createPablo(0);

        submitTx( tx -> {
            tx.put(pablo);
        });

        assertPabloVersion(0);
    }

    @Test
    public void putAtTime() {
        PersonDocument pablo = createPablo(0);

        submitTx( tx -> {
            tx.put(pablo, times.get(1));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0);
    }

    @Test
    public void putWithEndValidTime() {
        PersonDocument pablo = createPablo(0);

        submitTx( tx -> {
            tx.put(pablo, times.get(1), times.get(3));
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
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0, times.get(1));
            tx.put(pablo1, times.get(3));
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
        PersonDocument pablo = createPablo(0);

        submitTx ( tx -> {
            tx.put(pablo, times.get(1));
            tx.delete(pabloId);
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo();
    }

    @Test
    public void deleteAtSpecificTime() {
        PersonDocument pablo = createPablo(0);

        submitTx ( tx -> {
            tx.put(pablo, times.get(1));
            tx.delete(pabloId, times.get(3));
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
        PersonDocument pablo = createPablo(0);

        submitTx ( tx -> {
            tx.put(pablo, times.get(1));
            tx.delete(pabloId, times.get(3), times.get(5));
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
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0, times.get(1));
            tx.put(pablo1, times.get(5));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0, 3);
        assertPabloVersion(0, 4);
        assertPabloVersion(1, 5);
        assertPabloVersion(1, 6);
        assertPabloVersion(1);

        submitTx( tx -> {
            tx.delete(pabloId, times.get(3));
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
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx( tx -> {
            tx.put(pablo0);
        });

        assertPabloVersion(0);

        submitTx( tx -> {
            tx.match(pablo0);
            tx.put(pablo1);
        });

        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulMatchNow() {
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);
        PersonDocument pablo2 = createPablo(2);

        submitTx ( tx -> {
            tx.put(pablo0);
        });

        assertPabloVersion(0);

        submitTx ( tx -> {
            tx.match(pablo2);
            tx.put(pablo1);
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulMatchWithValidTime() {
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
           tx.put(pablo0, times.get(1), times.get(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx ( tx -> {
            tx.match(pablo0, times.get(2));
            tx.put(pablo1, times.get(3));
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
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0, times.get(1), times.get(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx ( tx -> {
            tx.match(pablo0, times.get(4));
            tx.put(pablo1, times.get(3));
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
        PersonDocument pablo = createPablo(0);

        assertNoPablo();

        submitTx ( tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo);
        });

        assertPabloVersion(0);
    }

    @Test
    public void unsuccessfulEmptyMatch() {
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0);
        });

        assertPabloVersion(0);

        submitTx ( tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo1);
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulEmptyMatchAtTime() {
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0, times.get(1), times.get(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx ( tx -> {
            tx.matchNotExists(pabloId, times.get(3));
            tx.put(pablo1, times.get(3));
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
        PersonDocument pablo0 = createPablo(0);
        PersonDocument pablo1 = createPablo(1);

        submitTx ( tx -> {
            tx.put(pablo0, times.get(1), times.get(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx ( tx -> {
            tx.matchNotExists(pabloId, times.get(2));
            tx.put(pablo1, times.get(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }
}