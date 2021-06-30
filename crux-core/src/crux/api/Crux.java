package crux.api;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.function.Consumer;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Public API entry point for starting an {@link ICruxAPI}.
 */
@SuppressWarnings("unused")
public final class Crux {

    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    private Crux() { }

    private static ICruxAPI startNode(Object config) {
        Object cruxNode = resolve("crux.api/start-node").invoke(config);
        return (ICruxAPI) resolve("crux.api.java/->JCruxNode").invoke(cruxNode);
    }

    /**
     * Starts an in-memory query node.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @return the started node
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAPI startNode() {
        return startNode(NodeConfiguration.EMPTY);
    }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param options a Map of Crux configuration
     * @return the started node.
     * @throws IndexVersionOutOfSyncException if the index needs rebuilding.
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAPI startNode(Map<?, ?> options) throws IndexVersionOutOfSyncException {
        return startNode((Object) options);
    }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param file a JSON or EDN file containing Crux configuration
     * @return the started node.
     * @throws IndexVersionOutOfSyncException if the index needs rebuilding.
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAPI startNode(File file) throws IndexVersionOutOfSyncException {
        return startNode((Object) file);
    }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param url a URL of a JSON or EDN file containing Crux configuration
     * @return the started node.
     * @throws IndexVersionOutOfSyncException if the index needs rebuilding.
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAPI startNode(URL url) throws IndexVersionOutOfSyncException {
        return startNode((Object) url);
    }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * <pre>
     * ICruxAPI cruxNode = Crux.startNode(n -&gt; {
     *   // ...
     * });
     * </pre>
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param f a callback, provided with an object to configure the node before it starts.
     * @return the started node.
     * @throws IndexVersionOutOfSyncException if the index needs rebuilding.
     * @see <a href="https://opencrux.com/reference/installation.html">Installation</a>
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAPI startNode(Consumer<NodeConfiguration.Builder> f) throws IndexVersionOutOfSyncException {
        return startNode(NodeConfiguration.buildNode(f));
    }
    public static ICruxAPI startNode(NodeConfiguration configuration) throws IndexVersionOutOfSyncException {
        return startNode(configuration.toMap());
    }

    /**
     * Creates a new remote API client.
     * <p>
     * NOTE: requires crux-http-client on the classpath.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param url the URL to a Crux HTTP end-point.
     * @return    a remote API client.
     */
    public static ICruxAPI newApiClient(String url) {
        Object apiClient = resolve("crux.remote-api-client/new-api-client").invoke(url);
        return (ICruxAPI) resolve("crux.api.java/->JCruxNode").invoke(apiClient);
    }

    /**
     * Creates a new remote API client.
     * <p>
     * NOTE: requires crux-http-client on the classpath.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param url the URL to a Crux HTTP end-point.
     * @param options options for the remote client.
     * @return    a remote API client.
     */
    public static ICruxAPI newApiClient(String url, RemoteClientOptions options) {
        Object apiClient = resolve("crux.remote-api-client/new-api-client").invoke(url, options);
        return (ICruxAPI) resolve("crux.api.java/->JCruxNode").invoke(apiClient);
    }

    /**
     * Starts an ingest-only client for transacting into Crux without
     * running a full local node with index.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param options node configuration options.
     * @return        the started ingest client node.
     * @see <a href="https://opencrux.com/reference/installation.html">Installation</a>
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAsyncIngestAPI newIngestClient(Map<?,?> options) {
        Object ingestClient = resolve("crux.ingest-client/open-ingest-client").invoke(options);
        return (ICruxAsyncIngestAPI) resolve("crux.api.java/->JCruxIngestClient").invoke(ingestClient);
    }

    /**
     * Starts an ingest-only client for transacting into Crux without
     * running a full local node with index.
     * <p>
     * <pre>
     * ICruxAPI ingestClient = Crux.newIngestClient(n -&gt; {
     *   // ...
     * });
     * </pre>
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param f a callback, provided with an object to configure the node before it starts.
     * @return the started ingest client node.
     * @see <a href="https://opencrux.com/reference/installation.html">Installation</a>
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    public static ICruxAsyncIngestAPI newIngestClient(Consumer<NodeConfiguration.Builder> f) {
        return newIngestClient(NodeConfiguration.buildNode(f));
    }
    public static ICruxAsyncIngestAPI newIngestClient(NodeConfiguration configuration) {
        return newIngestClient(configuration.toMap());
    }
}
