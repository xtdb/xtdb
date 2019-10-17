package crux.api;

import java.util.Map;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.IFn;

/**
 * Public API entry point for starting a {@link ICruxAPI}.
 */
public class Crux {

    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    private Crux() {
    }

    /**
     * Starts a query node.
     *
     * Options are specified as keywords using their long format name,
     * i.e. :crux.kakfa/bootstrap-servers. A valid :crux.node/topology
     * must be specified, i.e. :crux.kafka/topology. For valid option
     * descriptions, consult the documentation for the individual
     * modules used in the specified topology.
     *
     * Returns a crux.api.ICruxAPI component that implements
     * java.io.Closeable, which allows the node to be stopped by
     * calling close.
     *
     * @param options node configuration options.
     * @return        the started cluster node.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAPI startNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException {
        return (ICruxAPI) resolve("crux.node/start").invoke(options);
    }

    /**
     * Creates a new remote API client ICruxAPI. The remote client
     * requires valid and transaction time to be specified for all
     * calls to {@link ICruxAPI#db()}.
     *
     * NOTE: requires crux-http-client on the classpath,
     * see crux.remote-api-client/*internal-http-request-fn*
     * for more information.
     *
     * @param url the URL to a Crux HTTP end-point.
     * @return    a remote API client.
     */
    public static ICruxAPI newApiClient(String url) {
        return (ICruxAPI) resolve("crux.remote-api-client/new-api-client").invoke(url);
    }

    /**
     * Starts an ingest client for transacting into Kafka without
     * running a full local node with index.
     *
     * For valid options, see crux.kafka/default-options. Options are
     * specified as keywords using their long format name, like
     * :crux.kafka/bootstrap-servers etc.
     *
     * Returns a crux.api.ICruxAsyncIngestAPI component that
     * implements java.io.Closeable, which allows the client to be
     * stopped by calling close.
     *
     * @param options node configuration options.
     * @return        the started ingest client node.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAsyncIngestAPI newIngestClient(Map<Keyword,?> options) {
        return (ICruxAsyncIngestAPI) resolve("crux.kafka-ingest-client/new-ingest-client").invoke(options);
    }
}
