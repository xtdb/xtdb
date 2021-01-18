package crux.java;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import clojure.lang.PersistentArrayMap;
import org.junit.*;
import crux.api.*;

import java.util.Map;

import static crux.java.TestUtils.*;

public class TransactionInstantTest {
    private static final Keyword TX_ID = Keyword.intern("crux.tx/tx-id");
    private static final Keyword TX_TIME = Keyword.intern("crux.tx/tx-time");

    @Test
    public void nullFactoryTest() {
        TransactionInstant tx = TransactionInstant.factory((Map<Keyword, ?>)null);
        Assert.assertNull(tx);
    }

    @Test
    public void onlyTimeTest() {
        TransactionInstant tx = TransactionInstant.factory(now);
        Assert.assertNull(tx.getId());
        Assert.assertEquals(now, tx.getTime());
    }

    @Test
    public void onlyIdTest() {
        TransactionInstant tx = TransactionInstant.factory(1L);
        Assert.assertNull(tx.getTime());
        Assert.assertEquals(1L, (long) tx.getId());
    }

    @Test
    public void bothTest() {
        TransactionInstant tx = TransactionInstant.factory(1L, now);
        Assert.assertEquals(now, tx.getTime());
        Assert.assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapNeitherTest() {
        Map<Keyword, ?> map = PersistentArrayMap.EMPTY;
        TransactionInstant tx = TransactionInstant.factory(map);
        Assert.assertNull(tx);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapTimeTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY.assoc(TX_TIME, now);
        TransactionInstant tx = TransactionInstant.factory(map);
        Assert.assertNull(tx.getId());
        Assert.assertEquals(now, tx.getTime());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapIdTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY.assoc(TX_ID, 1L);
        TransactionInstant tx = TransactionInstant.factory(map);
        Assert.assertNull(tx.getTime());
        Assert.assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapBothTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY
                .assoc(TX_TIME, now)
                .assoc(TX_ID, 1L);

        TransactionInstant tx = TransactionInstant.factory(map);

        Assert.assertEquals(now, tx.getTime());
        Assert.assertEquals(1L, (long) tx.getId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void mapIsFineWithExtraneousTest() {
        Map<Keyword, ?> map = (Map<Keyword, ?>) PersistentArrayMap.EMPTY
                .assoc(TX_TIME, now)
                .assoc(TX_ID, 1L)
                .assoc(Keyword.intern("foo"), "bar");

        TransactionInstant tx = TransactionInstant.factory(map);

        Assert.assertEquals(now, tx.getTime());
        Assert.assertEquals(1L, (long) tx.getId());
    }
}
