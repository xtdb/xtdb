(ns xtdb.node
  "This namespace is for starting an in-process XTDB node.

  It lives in the `com.xtdb/xtdb-core` artifact - ensure you've included this in your dependency manager of choice to use in-process nodes."
  (:require [clojure.tools.logging :as log]
            [xtdb.time :as time])
  (:import [java.time ZoneId]
           [xtdb.api Xtdb Xtdb$Config XtdbSubmitClient XtdbSubmitClient$Config]))

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

(defmethod apply-config! :indexer [^Xtdb$Config config _ {:keys [rows-per-chunk page-limit log-limit flush-duration]}]
  (cond-> (.indexer config)
    rows-per-chunk (.rowsPerChunk rows-per-chunk)
    page-limit (.pageLimit page-limit)
    log-limit (.logLimit log-limit)
    flush-duration (.flushDuration (time/->duration flush-duration))))

(defmethod apply-config! :http-server [config _ opts]
  (apply-config! config :xtdb/server opts))

(defmethod apply-config! :flight-sql-server [config _ opts]
  (apply-config! config :xtdb.flight-sql/server opts))

(defmethod apply-config! :pgwire-server [config _ opts]
  (apply-config! config :xtdb.pgwire/server opts))

(defmethod apply-config! ::default [_ _ _])

(defn start-node
  "Starts an in-process node with the given configuration.

  For a simple, in-memory node (e.g. for testing/experimentation), you can elide the configuration map.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/sdks/clojure/index.html)"
  (^xtdb.api.IXtdb [] (start-node {}))

  (^xtdb.api.IXtdb [opts]
   (Xtdb/openNode (-> (doto (Xtdb$Config.)
                        (as-> config (reduce-kv (fn [config k v]
                                                  (doto config
                                                    (apply-config! k v)))
                                                config
                                                opts)))

                      (.extraConfig (dissoc opts
                                            :log
                                            :storage
                                            :indexer
                                            :default-tz
                                            :http-server
                                            :flight-sql-server
                                            :pgwire-server))))))

(defn start-submit-client
  "Starts a submit-only client with the given configuration.

  This client *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/sdks/clojure/index.html)"
  ^xtdb.api.IXtdbSubmitClient [opts]
  (XtdbSubmitClient/openSubmitClient (doto (XtdbSubmitClient$Config.)
                                       (as-> config (reduce-kv (fn [config k v]
                                                                 (doto config
                                                                   (apply-config! k v)))
                                                               config
                                                               opts)))))
