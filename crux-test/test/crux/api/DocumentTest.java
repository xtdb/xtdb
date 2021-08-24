package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import clojure.lang.PersistentArrayMap;
import crux.api.tx.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static crux.api.TestUtils.*;
import static org.junit.Assert.*;

import static crux.api.CruxDocument.build;

public class DocumentTest {
    private static final Keyword foo = Keyword.intern("foo");
    private static final Keyword bar = Keyword.intern("bar");

    @Test(expected = RuntimeException.class)
    public void factoryMissingId() {
        IPersistentMap data = PersistentArrayMap.EMPTY.assoc("foo", "bar");
        CruxDocument.factory(data);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reassigningId() {
        CruxDocument document = CruxDocument.create("foo");
        document.plus("xt/id", "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void reassigningFnId() {
        CruxDocument document = CruxDocument.create("foo");
        document.plus("crux.db/fn", "bar");
    }

    @Test
    public void simpleCreate() {
        HashMap<Keyword, Object> compare = new HashMap<>();

        compare.put(DB_ID, documentId);
        CruxDocument document = CruxDocument.create(documentId);
        CruxDocument builtDocument = build(documentId, doc -> {});

        assertEquals(compare, document.toMap());
        assertEquals(compare, builtDocument.toMap());
        assertEquals(document, builtDocument);

        assertSameAfterPut(document);
    }

    @Test
    public void createFromMap() {
        HashMap<Keyword, Object> compare = new HashMap<>();
        HashMap<String, Object> data = new HashMap<>();

        compare.put(DB_ID, documentId);

        data.put("foo", "bar");
        compare.put(foo, "bar");

        data.put("bar", 0);
        compare.put(bar, 0);

        CruxDocument document = CruxDocument.create(documentId, data);

        assertEquals(compare, document.toMap());
        assertSameAfterPut(document);
    }

    @Test
    public void plus() {
        HashMap<Keyword, Object> compare = new HashMap<>();

        compare.put(DB_ID, documentId);
        compare.put(foo, "bar");

        CruxDocument document = CruxDocument.create(documentId).plus("foo", "bar");
        CruxDocument builtDocument = build(documentId, doc -> {
            doc.put("foo", "bar");
        });

        assertEquals(compare, document.toMap());
        assertEquals(compare, builtDocument.toMap());
        assertEquals(document, builtDocument);
        assertSameAfterPut(document);
    }

    @Test
    public void plusAll() {
        HashMap<Keyword, Object> compare = new HashMap<>();
        HashMap<String, Object> data = new HashMap<>();

        compare.put(DB_ID, documentId);

        compare.put(foo, "bar");
        data.put("foo", "bar");
        compare.put(bar, 0);
        data.put("bar", 0);

        CruxDocument document = CruxDocument.create(documentId).plusAll(data);
        CruxDocument builtDocument = build(documentId, doc -> {
            doc.putAll(data);
        });

        assertEquals(compare, document.toMap());
        assertEquals(compare, builtDocument.toMap());
        assertEquals(document, builtDocument);
        assertSameAfterPut(document);
    }

    @Test
    public void minus() {
        HashMap<Keyword, Object> compare = new HashMap<>();
        HashMap<String, Object> data = new HashMap<>();
        compare.put(DB_ID, documentId);

        compare.put(foo, "bar");
        data.put("foo", "bar");

        data.put("bar", 0);

        CruxDocument document = CruxDocument.create(documentId, data).minus("bar");
        CruxDocument builtDocument = build(documentId, doc -> {
            doc.putAll(data);
            doc.remove("bar");
        });

        assertEquals(compare, document.toMap());
        assertEquals(compare, builtDocument.toMap());
        assertEquals(document, builtDocument);
        assertSameAfterPut(document);
    }

    @Test
    public void minusAll() {
        HashMap<Keyword, Object> compare = new HashMap<>();
        HashMap<String, Object> data = new HashMap<>();

        compare.put(DB_ID, documentId);

        data.put("foo", "bar");
        data.put("bar", 0);

        CruxDocument document = CruxDocument.create(documentId, data).minusAll(data.keySet());
        CruxDocument builtDocument = build(documentId, doc -> {
            doc.putAll(data);
            doc.removeAll(data.keySet());
        });

        assertEquals(compare, document.toMap());
        assertEquals(compare, builtDocument.toMap());
        assertEquals(document, builtDocument);
        assertSameAfterPut(document);
    }

    @Test
    public void functions() {
        CruxDocument document = CruxDocument.createFunction(documentId,
                "(fn [ctx eid] (let [db (crux.api/db ctx) entity (crux.api/entity db eid)] [[:crux.tx/put (update entity :person/version inc)]]))");
        assertSameAfterPut(document);
    }

    @Test
    public void immutibility() {
        CruxDocument document1 = CruxDocument.create(documentId);
        CruxDocument document2 = document1.plus("foo", "bar");
        CruxDocument document3 = document2.plus("bar", 0);
        CruxDocument document4 = document2.minus("foo");

        assertNotEquals(document1, document2);
        assertNotEquals(document1, document3);
        assertEquals(document1, document4);
        assertNotEquals(document2, document3);
    }

    @Test
    public void havingAFunctionDocumentWontBreakPlussing() {
        CruxDocument document = CruxDocument.createFunction("foo", "bar");
        document.plus("baz", 7);
    }

    private void assertSameAfterPut(CruxDocument document) {
        try (ICruxAPI node = Crux.startNode()) {
            TransactionInstant transaction = node.submitTx(Transaction.buildTx(tx -> {
                tx.put(document);
            }));
            awaitTx(node, transaction);
            CruxDocument compare = node.db().entity(document.getId());
            assertEquals(document, compare);
        }
        catch (Exception e) {
            fail();
        }
    }
}
