(ns crux-ui-server.crux-auto-start)


(def ^:private node-opts
  {:crux.node/topology :crux.standalone/topology
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.kv/kv-backend :crux.kv.rocksdb/kv
   :crux.kv/db-dir "data/db-dir-1"})

(def ^:private http-opts
  {:server-port 8080
   :cors-access-control
   [:access-control-allow-origin [#".*"]
    :access-control-allow-headers ["X-Requested-With"
                                   "Content-Type"
                                   "Cache-Control"
                                   "Origin"
                                   "Accept"
                                   "Authorization"
                                   "X-Custom-Header"]
    :access-control-allow-methods [:get :options :head :post]]})


(defn try-start-servers [{:keys [embed-crux crux-http-server-port]}]
  (try
    (let [start-node (requiring-resolve 'crux.api/start-node)
          start-http-server (requiring-resolve 'crux.http-server/start-http-server)
          node (start-node node-opts)
          http-opts (assoc http-opts :server-port crux-http-server-port)
          crux-http-server (start-http-server node http-opts)]
      {:crux-node node
       :crux-http-server crux-http-server})
    (catch ClassNotFoundException e
      (println "failed to load crux.http-server or crux.api, are they on the classpath?")
      {})))
