package crux.api;

import crux.api.exception.CruxIdException;
import crux.api.transaction.operation.DeleteTransactionOperation;
import crux.api.transaction.operation.EvictTransactionOperation;
import crux.api.transaction.operation.MatchTransactionOperation;
import org.junit.Test;

public class TransactionBuildersTest {
    @Test
    public void validIdDelete() {
        DeleteTransactionOperation.factory("foo");
    }

    @Test(expected = CruxIdException.class)
    public void invalidIdDelete() {
        DeleteTransactionOperation.factory(2.3);
    }

    @Test
    public void validIdEvict() {
        EvictTransactionOperation.factory("foo");
    }

    @Test(expected = CruxIdException.class)
    public void invalidIdEvict() {
        EvictTransactionOperation.factory(2.3);
    }

    @Test
    public void validIdMatch() {
        MatchTransactionOperation.factoryNotExists("foo");
    }

    @Test(expected = CruxIdException.class)
    public void invalidIdMatch() {
        MatchTransactionOperation.factoryNotExists(2.3);
    }
}
