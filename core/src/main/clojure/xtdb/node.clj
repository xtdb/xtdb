(ns xtdb.node
  "This namespace is for starting an in-process XTDB node.

  It lives in the `com.xtdb/xtdb-core` artifact - ensure you've included this in your dependency manager of choice to use in-process nodes."
  (:require [clojure.tools.logging :as log]
            [xtdb.time :as time])
  (:import [java.io File]
           [java.nio.file Path]
           [java.time ZoneId]
           [xtdb.api Xtdb Xtdb$Config]))

(defmulti ^:no-doc apply-config!
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [config k v]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]

        (try
          (require k)
          (catch Throwable _))))

    k)

  :default ::default)

(defmethod apply-config! :default-tz [^Xtdb$Config config _ default-tz]
  (when default-tz
    (.defaultTz config (cond
                         (instance? ZoneId default-tz) default-tz
                         (string? default-tz) (ZoneId/of default-tz)))))

(defmethod apply-config! :log [config _ opts]
  (apply-config! config :xtdb/log opts))

(defmethod apply-config! :storage [config _ opts]
  (apply-config! config :xtdb.buffer-pool/storage opts))

(defmethod apply-config! :indexer [^Xtdb$Config config _ {:keys [rows-per-block page-limit log-limit flush-duration skip-txs]}]
  (cond-> (.getIndexer config)
    rows-per-block (.rowsPerBlock rows-per-block)
    page-limit (.pageLimit page-limit)
    log-limit (.logLimit log-limit)
    flush-duration (.flushDuration (time/->duration flush-duration))
    skip-txs (.skipTxs skip-txs)))

(defmethod apply-config! :compactor [^Xtdb$Config config _ {:keys [threads]}]
  (cond-> (.getCompactor config)
    (some? threads) (.threads threads)))

(defmethod apply-config! :authn [config _ opts]
  (apply-config! config :xtdb/authn opts))

(defmethod apply-config! :http-server [config _ opts]
  (apply-config! config :xtdb/server opts))

(defmethod apply-config! :flight-sql-server [config _ opts]
  (apply-config! config :xtdb.flight-sql/server opts))

(defmethod apply-config! :server [config _ opts]
  (apply-config! config :xtdb.pgwire/server opts))

(defmethod apply-config! :healthz [config _ opts]
  (apply-config! config :xtdb/healthz opts))

(defmethod apply-config! :garbage-collector [config _ opts]
  (apply-config! config :xtdb/garbage-collector opts))

(defmethod apply-config! ::default [_ k _]
  (log/warn "Unknown configuration key:" k))

(defn ->config ^xtdb.api.Xtdb$Config [opts]
  (cond
    (instance? Xtdb$Config opts) opts
    (instance? Path opts) (Xtdb/readConfig ^Path opts)
    (instance? File opts) (->config (.toPath ^File opts))
    (map? opts) (doto (Xtdb$Config.)
                  (as-> config (reduce-kv (fn [config k v]
                                            (doto config
                                              (apply-config! k v)))
                                          config
                                          opts)))))

(defn start-node
  "Starts an in-process node with the given configuration.

  Accepts various parameter types:
  - An 'edn' map containing configuration options for the node.
  - An instance of 'xtdb.api.Xtdb$Config'.
  - An instance of 'java.io.File' pointing to an existing '.yaml' configuration file.
  - An instance of 'java.nio.file.Path' pointing to an existing '.yaml' configuration file.

  For a simple, in-memory node (e.g. for testing/experimentation), you can elide the configuration altogether.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration options, see the relevant module pages in the [Clojure docs](https://docs.xtdb.com/drivers/clojure/codox/index.html)"
  (^xtdb.api.Xtdb [] (start-node {}))

  (^xtdb.api.Xtdb [opts]
   (Xtdb/openNode (->config opts))))

(defn start-compactor
  "Starts an in-process compactor-only node with the given configuration.

  Accepts various parameter types:
  - An 'edn' map containing configuration options for the node.
  - An instance of 'xtdb.api.Xtdb$Config'.
  - An instance of 'java.io.File' pointing to an existing '.yaml' configuration file.
  - An instance of 'java.nio.file.Path' pointing to an existing '.yaml' configuration file.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration options, see the relevant module pages in the [Clojure docs](https://docs.xtdb.com/drivers/clojure/codox/index.html)"
  ^xtdb.api.Xtdb$CompactorNode [opts]
  (.openCompactor (->config opts)))
