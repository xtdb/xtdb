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

    private Crux() {
    }

    static {
        requiringResolve.invoke(Clojure.read("crux.standalone/topology"));
    }

    /**
     * Starts a query node.
     *
     * Returns a crux.api.ICruxAPI component that implements
     * java.io.Closeable, which allows the node to be stopped by
     * calling close.
     *
     * @param options TODO, how to specify options?
     * @return        the started cluster node.
     * @throws IndexVersionOutOfSyncException if the index needs
     * rebuilding.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAPI startNode(Map<Keyword,?> options) throws IndexVersionOutOfSyncException {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.node"));
        return (ICruxAPI) Clojure.var("crux.node/start").invoke(options);
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
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.remote-api-client"));
        return (ICruxAPI) Clojure.var("crux.remote-api-client/new-api-client").invoke(url);
    }

    /**
     * Starts an ingest client for transacting into Kafka without
     * running a full local node with index.
     *
     * For valid options, see crux.cli/cli-options. Options are
     * specified as keywords using their long format name, like
     * :bootstrap-servers etc.
     *
     * Returns a crux.api.ICruxAsyncIngestAPI component that
     * implements java.io.Closeable, which allows the client to be
     * stopped by calling close.
     *
     * @param options see crux.cli/cli-options.
     * @return        the started ingest client node.
     */
    @SuppressWarnings("unchecked")
    public static ICruxAsyncIngestAPI newIngestClient(Map<Keyword,?> options) {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.kafka-ingest-client"));
        return (ICruxAsyncIngestAPI) Clojure.var("crux.kafka-ingest-client/new-ingest-client").invoke(options);
    }
}
