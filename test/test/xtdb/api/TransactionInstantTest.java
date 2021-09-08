package xtdb.api;

import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Map;

import xtdb.api.*;
import xtdb.api.tx.*;

import org.junit.*;

import static org.junit.Assert.*;
import static xtdb.api.TestUtils.*;

public class TransactionInstantTest {
    private static final Keyword TX_ID = Keyword.intern("xtdb.api/tx-id");
    private static final Keyword TX_TIME = Keyword.intern("xtdb.api/tx-time");

    @Test
    public void nullFactoryTest() {
        TransactionInstant tx = TransactionInstant.factory((Map<Keyword, ?>)null);
        assertNull(tx);
    }

    @Test
    public void onlyTimeTest() {
        TransactionInstant tx = TransactionInstant.factory(now);
        assertNull(tx.getId());
        assertEquals(now, tx.getTime());
    }

    @Test
    public void onlyIdTest() {
        TransactionInstant tx = TransactionInstant.factory(1L);
        assertNull(tx.getTime());
        assertEquals(1L, (long) tx.getId());
    }

    @Test
    public void bothTest() {
        TransactionInstant tx = TransactionInstant.factory(1L, now);
        assertEquals(now, tx.getTime());
        assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapNeitherTest() {
        Map<Keyword, ?> map = PersistentArrayMap.EMPTY;
        TransactionInstant tx = TransactionInstant.factory(map);
        assertNull(tx);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapTimeTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY.assoc(TX_TIME, now);
        TransactionInstant tx = TransactionInstant.factory(map);
        assertNull(tx.getId());
        assertEquals(now, tx.getTime());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapIdTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY.assoc(TX_ID, 1L);
        TransactionInstant tx = TransactionInstant.factory(map);
        assertNull(tx.getTime());
        assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapBothTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY
                .assoc(TX_TIME, now)
                .assoc(TX_ID, 1L);

        TransactionInstant tx = TransactionInstant.factory(map);

        assertEquals(now, tx.getTime());
        assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapIsFineWithExtraneousTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY
                .assoc(TX_TIME, now)
                .assoc(TX_ID, 1L)
                .assoc(Keyword.intern("foo"), "bar");

        TransactionInstant tx = TransactionInstant.factory(map);

        assertEquals(now, tx.getTime());
        assertEquals(1L, (long) tx.getId());
    }
}
