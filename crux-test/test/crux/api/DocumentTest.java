package crux.api;

import clojure.lang.Keyword;

import crux.api.tx.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static crux.api.TestUtils.*;
import static org.junit.Assert.*;

public class DocumentTest {

    @Test(expected = RuntimeException.class)
    public void factoryMissingId() {
        HashMap<Keyword, Object> data = new HashMap<>();
        data.put(Keyword.intern("foo"), "bar");
        CruxDocument.factory(data);
    }

    @Test(expected = RuntimeException.class)
    public void reassigningId() {
        CruxDocument document = CruxDocument.create("foo");
        document.put("crux.db/id", "bar");
    }

    @Test
    public void checkingConsistency() {
        HashMap<Keyword, Object> compare = new HashMap<>();
        compare.put(AbstractCruxDocument.DB_ID, "foo");
        CruxDocument document = CruxDocument.create("foo");

        compare.put(Keyword.intern("bar"), "baz");
        document.put("bar", "baz");

        assertEquals(compare, document.toMap());

        assertSameAfterPut(document);
    }

    @Test
    public void customImplementation() {
        AbstractCruxDocument myDocument = new AbstractCruxDocument() {
            @Override
            public Object getId() {
                return "foo";
            }

            @Override
            protected Map<Keyword, Object> getData() {
                HashMap<Keyword, Object> ret = new HashMap<>();
                ret.put(Keyword.intern("bar"), "baz");
                return ret;
            }
        };

        CruxDocument document = CruxDocument.create("foo");
        document.put("bar", "baz");

        assertEquals(document, myDocument);

        assertSameAfterPut(document);
        assertSameAfterPut(myDocument);
    }

    private void assertSameAfterPut(AbstractCruxDocument document) {
        try (ICruxAPI node = Crux.startNode()) {
            TransactionInstant transaction = node.submitTx(Transaction.buildTx(tx -> {
                tx.put(document);
            }));
            awaitTx(node, transaction);
            AbstractCruxDocument compare = node.db().entity(document.getId());
            assertEquals(document, compare);
        }
        catch (Exception e) {
            fail();
        }
    }
}
