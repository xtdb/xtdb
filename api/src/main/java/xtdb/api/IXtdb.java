package xtdb.api;

import xtdb.query.Query;
import xtdb.query.QueryOpts;
import xtdb.tx.Ops;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public interface IXtdb extends AutoCloseable {

    CompletableFuture<Stream<Map<String, ?>>> openQueryAsync(Query q, QueryOpts opts);

    private static <T> T await(CompletableFuture<T> fut) {
        try {
            return fut.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    default CompletableFuture<Stream<Map<String, ?>>> openQueryAsync(Query q) {
        return openQueryAsync(q, new QueryOpts());
    }

    default Stream<Map<String, ?>> openQuery(Query q) {
        return await(openQueryAsync(q));
    }

    default Stream<Map<String, ?>> openQuery(Query q, QueryOpts opts) {
        return await(openQueryAsync(q, opts));
    }

    CompletableFuture<TransactionKey> submitTxAsync(List<Ops> ops, TxOptions txOpts);

    default CompletableFuture<TransactionKey> submitTxAsync(List<Ops> ops) {
        return submitTxAsync(ops, new TxOptions());
    }

    default TransactionKey submitTx(List<Ops> ops, TxOptions txOpts) {
        return await(submitTxAsync(ops, txOpts));
    }

    default TransactionKey submitTx(List<Ops> ops) {
        return await(submitTxAsync(ops));
    }

    @Override
    void close();
}
