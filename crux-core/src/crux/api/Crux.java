package crux.api;

import java.util.Map;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.IFn;

/**
 * Public API entry point for starting a {@link ICruxAPI}.
 */
public class Crux {
    private Crux() {
    }

    /**
     * Starts a query node in local library mode.
     *
     * For valid options, see crux.bootstrap/cli-options. Options are
     * specified as keywords using their long format name, like
     * :bootstrap-servers etc.
     *
     * Returns a crux.api.ICruxAPI component that implements
     * java.io.Closeable, which allows the node to be stopped by
     * calling close.
     *
     * NOTE: requires any KV store dependencies and crux-kafka on
     * the classpath. The crux.kv.memdb.MemKv KV backend works without
     * additional dependencies.
     *
     * The HTTP API can be started by passing the node to
     * crux.http-server/start-http-server. This will require further
     * dependencies on the classpath, see crux.http-server for
     * details.
     *
     * See also crux.kafka.embedded or {@link
     * #startStandaloneNode(Map options)} for self-contained
     * deployments.
     *
     * @param options see crux.bootstrap/cli-options.
     * @return        the started cluster node.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAPI startClusterNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.kafka"));
        IFn deref = Clojure.var("clojure.core", "deref");
        Map<Keyword,?> nodeConfig = (Map<Keyword,?>) deref.invoke(Clojure.var("crux.kafka/node-config"));
        return (ICruxAPI) Clojure.var("crux.bootstrap/start-node").invoke(nodeConfig, options);
    }

    /**
     * Creates a minimal standalone node writing the transaction log
     * into its local KV store without relying on
     * Kafka. Alternatively, when the event-log-dir option is
     * provided, using two KV stores to enable rebuilding the index
     * from the event log, being more similar to the semantics of
     * Kafka but for a single process only.

     * Returns a ICruxAPI component that implements
     * java.io.Closeable, which allows the node to be stopped by
     * calling close.

     * NOTE: requires any KV store dependencies on the classpath. The
     * crux.kv.memdb.MemKv KV backend works without additional dependencies.
     *
     * @param options see crux.bootstrap/start-kv-store.
     * @return        a standalone node.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     * @throws NonMonotonicTimeException if the clock has moved
     * backwards since last run. Only applicable when using the event
     * log.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAPI startStandaloneNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException, NonMonotonicTimeException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.standalone"));
        IFn deref = Clojure.var("clojure.core", "deref");
        Map<Keyword,?> nodeConfig = (Map<Keyword,?>) deref.invoke(Clojure.var("crux.standalone/node-config"));
        return (ICruxAPI) Clojure.var("crux.bootstrap/start-node").invoke(nodeConfig, options);
    }

    /**
     * Starts a query node in local library mode using JDBC.
     *
     * Returns a ICruxAPI component that implements
     * java.io.Closeable, which allows the node to be stopped by
     * calling close.
     *
     * @param options see crux.jdbc
     * @return        a cluster node backed by JDBC.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAPI startJDBCNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.jdbc"));
        IFn deref = Clojure.var("clojure.core", "deref");
        Map<Keyword,?> nodeConfig = (Map<Keyword,?>) deref.invoke(Clojure.var("crux.jdbc/node-config"));
        return (ICruxAPI) Clojure.var("crux.bootstrap/start-node").invoke(nodeConfig, options);
    }

    /**
     * Creates a new remote API client ICruxAPI. The remote client
     * requires valid and transaction time to be specified for all
     * calls to {@link ICruxAPI#db()}.
     *
     * NOTE: requires crux-http-client on the classpath,
     * see crux.bootstrap.remove-api-client/*internal-http-request-fn*
     * for more information.
     *
     * @param url the URL to a Crux HTTP end-point.
     * @return    a remote API client.
     */
    public static ICruxAPI newApiClient(String url) {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.bootstrap.remote-api-client"));
        return (ICruxAPI) Clojure.var("crux.bootstrap.remote-api-client/new-api-client").invoke(url);
    }

    /**
     * Starts an ingest client for transacting into Kafka without
     * running a full local node with index.
     *
     * For valid options, see crux.bootstrap/cli-options. Options are
     * specified as keywords using their long format name, like
     * :bootstrap-servers etc.
     *
     * Returns a crux.api.ICruxAsyncIngestAPI component that
     * implements java.io.Closeable, which allows the client to be
     * stopped by calling close.
     *
     * @param options see crux.bootstrap/cli-options.
     * @return        the started ingest client node.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAsyncIngestAPI newIngestClient(Map<Keyword,?> options) {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.bootstrap.kafka-ingest-client"));
        return (ICruxAsyncIngestAPI) Clojure.var("crux.bootstrap.kafka-ingest-client/new-ingest-client").invoke(options);
    }
}
