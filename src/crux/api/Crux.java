package crux.api;

import java.util.Map;

import clojure.java.api.Clojure;

public class Crux {
    /**
     * Starts a query node in local library mode.
     *
     * For valid options, see crux.bootstrap/cli-options. Options are
     * specified as keywords using their long format name, like
     * :bootstrap-servers etc.
     *
     * Returns a crux.api.LocalNode component that implements
     * java.io.Closeable, which allows the system to be stopped by
     * calling close.
     *
     * NOTE: requires any KV store dependencies on the classpath. The
     * crux.memdb.MemKv KV backend works without additional
     * dependencies.
     *
     * The HTTP API can be started by passing the LocalNode to
     * crux.http-server/start-http-server.  This will require further
     * dependencies on the classpath, see crux.http-server for
     * details.
     *
     * See also crux.kafka.embedded or crux.API#newStandaloneSystem.
     * for self-contained deployments.
     */
    public static ICruxSystem startLocalNode(Map options) {
        return (ICruxSystem) Clojure.var("crux.api/start-local-node").invoke(options);
    }

    /**
     * Creates a minimal standalone system writing the transaction log
     * into its local KV store without relying on Kafka.

     * Returns a crux.api.StandaloneSystem component that implements
     * java.io.Closeable, which allows the system to be stopped by
     * calling close.

     * NOTE: requires any KV store dependencies on the classpath. The
     * crux.memdb.MemKv KV backend works without additional dependencies.
     */
    public static ICruxSystem newStandaloneSystem(Map options) {
        return (ICruxSystem) Clojure.var("crux.api/new-standalone-system").invoke(options);
    }

    /**
     * Creates a new remote API client CruxSystem.
     *
     * NOTE: requires either clj-http or http-kit on the classpath.
     */
    public static ICruxSystem newApiClient(String url) {
        return (ICruxSystem) Clojure.var("crux.api/new-api-client").invoke(url);
    }
}
