package crux.java;

import clojure.lang.Keyword;

import crux.api.*;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;

import org.junit.*;

class TestUtils {
    static final Keyword PUT = Keyword.intern("crux.tx/put");
    static final Keyword DELETE = Keyword.intern("crux.tx/delete");

    static final Keyword DB_ID = Keyword.intern("crux.db/id");
    static final Keyword VALID_TIME = Keyword.intern("crux.db/valid-time");
    static final Keyword CONTENT_HASH = Keyword.intern("crux.db/content-hash");
    static final Keyword DOC = Keyword.intern("crux.db/doc");

    static final Keyword TX_TIME = Keyword.intern("crux.tx/tx-time");
    static final Keyword TX_ID = Keyword.intern("crux.tx/tx-id");
    static final Keyword TX_EVENTS = Keyword.intern("crux.tx.event/tx-events");
    static final Keyword TX = Keyword.intern("crux.tx/tx");

    static final Keyword TX_OPS = Keyword.intern("crux.api/tx-ops");

    static final Duration duration = Duration.ofSeconds(10);

    static final Date now = new Date();

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static List<Object> listOf(Object... objects) {
        return Arrays.asList(objects);
    }

    static Date date(long diff) {
        return Date.from(now.toInstant().plusMillis(diff));
    }

    static long invertDate(Date date) {
        return now.getTime() - date.getTime();
    }

    static TransactionInstant tx(ICruxAPI node, List<?> txOp) {
        ArrayList<List<?>> toSubmit = new ArrayList<>();
        toSubmit.add(txOp);
        return node.submitTx((List<List<?>>) toSubmit);
    }

    static TransactionInstant put(ICruxAPI node, Map<Keyword, ?> document, Date validTime, Date endValidTime) {
        List<Object> txOp;
        if (endValidTime != null) {
            txOp = listOf(PUT, document, validTime, endValidTime);
        }
        else if (validTime != null) {
            txOp = listOf(PUT, document, validTime);
        }
        else {
            txOp = listOf(PUT, document);
        }

        return tx(node, txOp);
    }

    static TransactionInstant delete(ICruxAPI node, Object documentId, Date validTime, Date endValidTime) {
        List<Object> txOp;
        if (endValidTime != null) {
            txOp = listOf(DELETE, documentId, validTime, endValidTime);
        }
        else if (validTime != null) {
            txOp = listOf(DELETE, documentId, validTime);
        }
        else {
            txOp = listOf(DELETE, documentId);
        }

        return tx(node, txOp);
    }

    static void close(Closeable closeable) {
        try {
            closeable.close();
        }
        catch (Exception e) {
            Assert.fail();
        }
    }

    static void assertHasKeys(List<Map<Keyword, ?>> maps, Keyword... keys) {
        for (Map<Keyword, ?> map: maps) {
            assertHasKeys(map, keys);
        }
    }

    static void assertHasKeys(Map<Keyword, ?> map, Keyword... keys) {
        for (Keyword key: keys) {
            Assert.assertTrue(map.containsKey(key));
        }

        Assert.assertEquals(keys.length, map.size());
    }

    static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }
}
