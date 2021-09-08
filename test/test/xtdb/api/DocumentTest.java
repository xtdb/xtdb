package xtdb.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import clojure.lang.PersistentArrayMap;
import xtdb.api.tx.*;

import java.util.HashMap;

import org.junit.Test;

import static xtdb.api.TestUtils.*;
import static org.junit.Assert.*;

import static xtdb.api.XtdbDocument.build;

public class DocumentTest {
    private static final Keyword foo = Keyword.intern("foo");
    private static final Keyword bar = Keyword.intern("bar");

    @Test(expected = RuntimeException.class)
    public void factoryMissingId() {
        IPersistentMap data = PersistentArrayMap.EMPTY.assoc("foo", "bar");
        XtdbDocument.factory(data);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reassigningId() {
        XtdbDocument document = XtdbDocument.create("foo");
        document.plus("xt/id", "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void reassigningFnId() {
        XtdbDocument document = XtdbDocument.create("foo");
        document.plus("xt/fn", "bar");
    }

    @Test
    public void simpleCreate() {
        HashMap<Keyword, Object> compare = new HashMap<>();

        compare.put(DB_ID, documentId);
        XtdbDocument document = XtdbDocument.create(documentId);
        XtdbDocument builtDocument = build(documentId, doc -> {});

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

        XtdbDocument document = XtdbDocument.create(documentId, data);

        assertEquals(compare, document.toMap());
        assertSameAfterPut(document);
    }

    @Test
    public void plus() {
        HashMap<Keyword, Object> compare = new HashMap<>();

        compare.put(DB_ID, documentId);
        compare.put(foo, "bar");

        XtdbDocument document = XtdbDocument.create(documentId).plus("foo", "bar");
        XtdbDocument builtDocument = build(documentId, doc -> {
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

        XtdbDocument document = XtdbDocument.create(documentId).plusAll(data);
        XtdbDocument builtDocument = build(documentId, doc -> {
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

        XtdbDocument document = XtdbDocument.create(documentId, data).minus("bar");
        XtdbDocument builtDocument = build(documentId, doc -> {
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

        XtdbDocument document = XtdbDocument.create(documentId, data).minusAll(data.keySet());
        XtdbDocument builtDocument = build(documentId, doc -> {
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
        XtdbDocument document = XtdbDocument.createFunction(documentId,
                "(fn [ctx eid] (let [db (xtdb.api/db ctx) entity (xtdb.api/entity db eid)] [[:xtdb.api/put (update entity :person/version inc)]]))");
        assertSameAfterPut(document);
    }

    @Test
    public void immutibility() {
        XtdbDocument document1 = XtdbDocument.create(documentId);
        XtdbDocument document2 = document1.plus("foo", "bar");
        XtdbDocument document3 = document2.plus("bar", 0);
        XtdbDocument document4 = document2.minus("foo");

        assertNotEquals(document1, document2);
        assertNotEquals(document1, document3);
        assertEquals(document1, document4);
        assertNotEquals(document2, document3);
    }

    @Test
    public void havingAFunctionDocumentWontBreakPlussing() {
        XtdbDocument document = XtdbDocument.createFunction("foo", "bar");
        document.plus("baz", 7);
    }

    private void assertSameAfterPut(XtdbDocument document) {
        try (IXtdb node = IXtdb.startNode()) {
            TransactionInstant transaction = node.submitTx(Transaction.buildTx(tx -> {
                tx.put(document);
            }));
            awaitTx(node, transaction);
            XtdbDocument compare = node.db().entity(document.getId());
            assertEquals(document, compare);
        }
        catch (Exception e) {
            fail();
        }
    }
}
