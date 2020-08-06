package crux.api;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.IFn;

/**
 * Public API entry point for starting an {@link ICruxAPI}.
 */
public class Crux {

    private static IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");

    private static IFn resolve(String symbolName) {
        return (IFn) requiringResolve.invoke(Clojure.read(symbolName));
    }

    private Crux() { }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @param options node configuration options.
     * @return the started node.
     * @throws IndexVersionOutOfSyncException if the index needs rebuilding.
     * @see <a href="https://opencrux.com/reference/installation.html">Installation</a>
     * @see <a href="https://opencrux.com/reference/configuration.html">Configuration</a>
     */
    @SuppressWarnings("unused")
    public static ICruxAPI startNode(Map<Keyword, ?> options) throws IndexVersionOutOfSyncException {
        return (ICruxAPI) resolve("crux.node/start").invoke(options);
    }

    /**
     * Starts an in-memory query node.
     * <p>
     * When you're done, close the node with {@link java.io.Closeable#close}
     *
     * @return the started node
     * @see <a href="https://opencrux.com/reference/installation.html">Installation</a>
     */
    @SuppressWarnings("unused")
    public static ICruxAPI startNode() {
        return startNode(c -> {});
    }

    /**
     * Starts a Crux node using the provided configuration.
     * <p>
     * <pre>
     * ICruxAPI cruxNode = Crux.startNode(n -> {
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
    public static ICruxAPI startNode(Consumer<NodeConfigurator> f) throws IndexVersionOutOfSyncException {
        NodeConfigurator c = new NodeConfigurator();
        f.accept(c);
        return (ICruxAPI) resolve("crux.node/start").invoke(c.modules);
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
    @SuppressWarnings("unused")
    public static ICruxAPI newApiClient(String url) {
        return (ICruxAPI) resolve("crux.remote-api-client/new-api-client").invoke(url);
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
    @SuppressWarnings("unused")
    public static ICruxAPI newApiClient(String url, RemoteClientOptions options) {
        return (ICruxAPI) resolve("crux.remote-api-client/new-api-client").invoke(url, options);
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
    @SuppressWarnings("unused")
    public static ICruxAsyncIngestAPI newIngestClient(Map<Keyword,?> options) {
        return (ICruxAsyncIngestAPI) resolve("crux.ingest-client/open-ingest-client").invoke(options);
    }
}
