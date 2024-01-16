(ns xtdb.node
  "This namespace is for starting an in-process XTDB node.

  It lives in the `com.xtdb/xtdb-core` artifact - ensure you've included this in your dependency manager of choice to use in-process nodes."
  (:import [xtdb.api Xtdb Xtdb$Config]))

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

(defmethod apply-config! :xtdb.indexer/live-index [^Xtdb$Config config _
                                                   {:keys [rows-per-chunk page-limit log-limit]}]
  (cond-> (.indexer config)
    rows-per-chunk (.rowsPerChunk rows-per-chunk)
    page-limit (.pageLimit page-limit)
    log-limit (.logLimit log-limit)))

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
                                            :xtdb.buffer-pool/in-memory
                                            :xtdb.buffer-pool/local
                                            :xtdb.buffer-pool/remote
                                            :xtdb.log/memory-log
                                            :xtdb.log/local-directory-log
                                            :xtdb.indexer/live-index))))))

(defn start-submit-client
  "Starts a submit-only client with the given configuration.

  This client *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/sdks/clojure/index.html)"
  ^java.lang.AutoCloseable [opts]
  ((requiring-resolve 'xtdb.node.impl/start-submit-client) opts))
