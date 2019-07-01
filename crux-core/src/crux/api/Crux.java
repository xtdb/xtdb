package crux.api;

import java.util.Map;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;

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
     * java.io.Closeable, which allows the system to be stopped by
     * calling close.
     *
     * NOTE: requires any KV store dependencies and kafka-clients on
     * the classpath. The crux.kv.memdb.MemKv KV backend works without
     * additional dependencies.
     *
     * The HTTP API can be started by passing the system to
     * crux.http-server/start-http-server. This will require further
     * dependencies on the classpath, see crux.http-server for
     * details.
     *
     * See also crux.kafka.embedded or {@link
     * #startStandaloneSystem(Map options)} for self-contained
     * deployments.
     *
     * @param options see crux.bootstrap/cli-options.
     * @return        the started local node.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     */
    public static ICruxAPI startClusterNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.bootstrap.cluster-node"));
        return (ICruxAPI) Clojure.var("crux.bootstrap.cluster-node/start-cluster-node").invoke(options);
    }

    /**
     * Creates a minimal standalone system writing the transaction log
     * into its local KV store without relying on
     * Kafka. Alternatively, when the event-log-dir option is
     * provided, using two KV stores to enable rebuilding the index
     * from the event log, being more similar to the semantics of
     * Kafka but for a single process only.

     * Returns a ICruxAPI component that implements
     * java.io.Closeable, which allows the system to be stopped by
     * calling close.

     * NOTE: requires any KV store dependencies on the classpath. The
     * crux.kv.memdb.MemKv KV backend works without additional dependencies.
     *
     * @param options see crux.bootstrap/start-kv-store.
     * @return        a standalone system.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     * @throws NonMonotonicTimeException if the clock has moved
     * backwards since last run. Only applicable when using the event
     * log.
     */
    public static ICruxAPI startStandaloneSystem(Map<Keyword,?> options) throws IndexVersionOutOfSyncException, NonMonotonicTimeException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.bootstrap.standalone"));
        return (ICruxAPI) Clojure.var("crux.bootstrap.standalone/start-standalone-system").invoke(options);
    }

    /**
     * Creates a new remote API client ICruxAPI. The remote client
     * requires valid and transaction time to be specified for all
     * calls to {@link ICruxAPI#db()}.
     *
     * NOTE: requires either clj-http or http-kit on the classpath,
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
}
