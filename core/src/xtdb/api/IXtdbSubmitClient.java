package xtdb.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import clojure.lang.IDeref;
import clojure.lang.Keyword;
import xtdb.api.tx.Transaction;

import static xtdb.api.XtdbFactory.resolve;

/**
 * Provides API access to XTDB transaction submission.
 */
@SuppressWarnings("unused")
public interface IXtdbSubmitClient extends Closeable {

    /**
     * Starts an submit-only client for transacting into XTDB without
     * running a full local node with index.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param options node configuration options.
     * @return        the started submit client node.
     * @see <a href="https://v1-docs.xtdb.com/administration/configuring/">Configuring</a>
     */
    static IXtdbSubmitClient newSubmitClient(Map<?, ?> options) {
        Object submitClient = resolve("xtdb.submit-client/open-submit-client").invoke(options);
        return (IXtdbSubmitClient) resolve("xtdb.api.java/->JXtdbSubmitClient").invoke(submitClient);
    }

    /**
     * Starts an submit-only client for transacting into XTDB without
     * running a full local node with index.
     * <p>
     * <pre>
     * IXtdbSubmitClient submitClient = IXtdbSubmitClient.newSubmitClient(n -&gt; {
     *   // ...
     * });
     * </pre>
     * <p>
     * When you're done, close the close with {@link java.io.Closeable#close}
     *
     * @param f a callback, provided with an object to configure the node before it starts.
     * @return the started submit client node.
     * @see <a href="https://v1-docs.xtdb.com/administration/configuring/">Configuring</a>
     */
    static IXtdbSubmitClient newSubmitClient(Consumer<NodeConfiguration.Builder> f) {
        return newSubmitClient(NodeConfiguration.buildNode(f));
    }

    static IXtdbSubmitClient newSubmitClient(NodeConfiguration configuration) {
        return newSubmitClient(configuration.toMap());
    }

    /**
     * Writes transactions to the log for processing.
     *
     * @param transaction the transaction to be processed.
     * @return      a map with details about the submitted transaction.
     */
    TransactionInstant submitTx(Transaction transaction);

    /**
     * Writes transactions to the log for processing.
     *
     * @param transaction the transaction to be processed.
     * @return      a map with details about the submitted transaction.
     * @deprecated in favour of {@link #submitTx(Transaction)}
     */
    @Deprecated
    Map<Keyword, ?> submitTx(List<List<?>> transaction);

    /**
     * Reads the transaction log. Optionally includes  operations, which allow the contents
     * under the ::xt/tx-ops key to be piped into (submit-tx tx-ops) of another
     * XTDB instance.
     *
     * @param afterTxId optional transaction id to start after.
     * @param withOps   should the operations with documents be included?
     * @return          a lazy sequence of the transaction log.
     */
    ICursor<Map<Keyword, ?>> openTxLog(Long afterTxId, boolean withOps);

    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param txOps the transactions to be processed.
     * @return      a {@link CompletableFuture} with a map with details about the submitted transaction.
     */
    CompletableFuture<TransactionInstant> submitTxAsync(Transaction txOps);

    /**
     * Writes transactions to the log for processing. Non-blocking.
     *
     * @param txOps the transactions to be processed.
     * @return      a deref with a map with details about the submitted transaction.
     * @deprecated in favour of {@link #submitTxAsync(Transaction)}
     */
    @Deprecated
    IDeref submitTxAsync(List<List<?>> txOps);
}
