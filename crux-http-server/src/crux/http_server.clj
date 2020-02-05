(ns crux.http-server
  "HTTP API for Crux.

  The optional SPARQL handler requires juxt.crux/rdf."
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.tx :as tx]
            [ring.adapter.jetty :as j]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.params :as p]
            [ring.util.io :as rio]
            [ring.util.request :as req]
            [ring.util.time :as rt]
            [crux.api :as api])
  (:import [crux.api ICruxAPI ICruxDatasource NodeOutOfSyncException]
           [java.io Closeable IOException]
           java.time.Duration
           java.util.Date
           org.eclipse.jetty.server.Server))

;; ---------------------------------------------------
;; Utils

(defn- body->edn [request]
  (->> request
       req/body-string
       (c/read-edn-string-with-readers)))

(defn- check-path [[path-pattern valid-methods] request]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (re-find path-pattern path)
         (some #{method} valid-methods))))

(defn- response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn- success-response [m]
  (response (if (some? m) 200 404)
            {"Content-Type" "application/edn"}
            (cio/pr-edn-str m)))

(defn- exception-response [status ^Exception e]
  (response status
            {"Content-Type" "application/edn"}
            (with-out-str
              (pp/pprint (Throwable->map e)))))

(defn- wrap-exception-handling [handler]
  (fn [request]
    (try
      (try
        (handler request)
        (catch Exception e
          (if (and (.getMessage e)
                   (str/starts-with? (.getMessage e) "Spec assertion failed"))
            (exception-response 400 e) ;; Valid edn, invalid content
            (do (log/error e "Exception while handling request:" (cio/pr-edn-str request))
                (exception-response 500 e))))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

(defn- add-last-modified [response date]
  (cond-> response
    date (assoc-in [:headers "Last-Modified"] (rt/format-date date))))

;; ---------------------------------------------------
;; Services

(defn- status [^ICruxAPI crux-node]
  (let [status-map (api/status crux-node)]
    (if (or (not (contains? status-map :crux.zk/zk-active?))
            (:crux.zk/zk-active? status-map))
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (cio/pr-edn-str status-map)))))

(defn- document [^ICruxAPI crux-node request]
  (let [[_ content-hash] (re-find #"^/document/(.+)$" (req/path-info request))]
    (success-response (api/document crux-node (c/new-id content-hash)))))

(defn- stringify-keys [m]
  (persistent!
    (reduce-kv
      (fn [m k v] (assoc! m (str k) v))
      (transient {})
      m)))

(defn- documents [^ICruxAPI crux-node  {:keys [query-params] :as request}]
  ; TODO support for GET
  (let [preserve-ids? (Boolean/parseBoolean (get query-params "preserve-crux-ids" "false"))
        content-hashes-set (body->edn request)
        ids-set (set (map c/new-id content-hashes-set))]
    (success-response
      (cond-> (api/documents crux-node ids-set)
              (not preserve-ids?) stringify-keys))))

(defn- history [^ICruxAPI crux-node request]
  (let [[_ eid] (re-find #"^/history/(.+)$" (req/path-info request))
        history (api/history crux-node (c/new-id eid))]
    (-> (success-response history)
        (add-last-modified (:crux.tx/tx-time (first history))))))

(defn- parse-history-range-params [{:keys [query-params] :as request}]
  (let [[_ eid] (re-find #"^/history-range/(.+)$" (req/path-info request))
        times (map #(some-> (get query-params %) not-empty cio/parse-rfc3339-or-millis-date)
                   ["valid-time-start" "transaction-time-start" "valid-time-end" "transaction-time-end"])]
    (cons eid times)))

(defn- history-range [^ICruxAPI crux-node request]
  (let [[eid valid-time-start transaction-time-start valid-time-end transaction-time-end] (parse-history-range-params request)
        history (api/history-range crux-node (c/new-id eid) valid-time-start transaction-time-start valid-time-end transaction-time-end)
        last-modified (:crux.tx/tx-time (last history))]
    (-> (success-response history)
        (add-last-modified (:crux.tx/tx-time (last history))))))

(defn- db-for-request ^ICruxDatasource [^ICruxAPI crux-node {:keys [valid-time transact-time]}]
  (cond
    (and valid-time transact-time)
    (api/db crux-node valid-time transact-time)

    valid-time
    (api/db crux-node valid-time)

    ;; TODO: This could also be an error, depending how you see it,
    ;; not supported via the Java API itself.
    transact-time
    (api/db crux-node (cio/next-monotonic-date) transact-time)

    :else
    (api/db crux-node)))

(defn- streamed-edn-response [^Closeable edn]
  (try
    (->> (rio/piped-input-stream
          (fn [out]
            (with-open [edn edn
                        out (io/writer out)]
              (.write out "(")
              (doseq [x edn]
                (.write out (cio/pr-edn-str x)))
              (.write out ")"))))
         (response 200 {"Content-Type" "application/edn"}))
    (catch Throwable t
      (.close edn)
      (throw t))))

(def ^:private date? (partial instance? Date))
(s/def ::valid-time date?)
(s/def ::transact-time date?)

(s/def ::query-map (s/and #(set/superset? #{:query :valid-time :transact-time} (keys %))
                          (s/keys :req-un [:crux.query/query]
                                  :opt-un [::valid-time
                                           ::transact-time])))

;; TODO: Potentially require both valid and transaction time sent by
;; the client?
(defn- query [^ICruxAPI crux-node request]
  (let [query-map (s/assert ::query-map (body->edn request))
        db (db-for-request crux-node query-map)]
    (-> (success-response (api/q db (:query query-map)))
        (add-last-modified (api/transaction-time db)))))

(defn- query-stream [^ICruxAPI crux-node request]
  (let [query-map (s/assert ::query-map (body->edn request))
        db (db-for-request crux-node query-map)]
    (-> (streamed-edn-response (api/open-q db (:query query-map)))
        (add-last-modified (api/transaction-time db)))))

(s/def ::eid c/valid-id?)
(s/def ::entity-map (s/and #(set/superset? #{:eid :valid-time :transact-time} (keys %))
                           (s/keys :req-un [::eid]
                                   :opt-un [::valid-time
                                            ::transact-time])))

;; TODO: Could support as-of now via path and GET.
(defn- entity [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        {:keys [crux.tx/tx-time] :as entity-tx} (api/entity-tx db eid)]
    (-> (success-response (api/entity db eid))
        (add-last-modified tx-time))))

(defn- entity-tx [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        {:keys [crux.tx/tx-time] :as entity-tx} (api/entity-tx db eid)]
    (-> (success-response entity-tx)
        (add-last-modified tx-time))))

(defn- history-ascending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))]
    (-> (streamed-edn-response (api/open-history-ascending (db-for-request crux-node body) eid))
        (add-last-modified (:crux.tx/tx-time (api/latest-completed-tx crux-node))))))

(defn- history-descending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))]
    (-> (streamed-edn-response (api/open-history-descending (db-for-request crux-node body) eid))
        (add-last-modified (:crux.tx/tx-time (api/latest-completed-tx crux-node))))))

(defn- transact [^ICruxAPI crux-node request]
  (let [tx-ops (body->edn request)
        {:keys [crux.tx/tx-time] :as submitted-tx} (api/submit-tx crux-node tx-ops)]
    (-> (success-response submitted-tx)
        (assoc :status 202)
        (add-last-modified tx-time))))

;; TODO: Could add from date parameter.
(defn- tx-log [^ICruxAPI crux-node request]
  (let [with-ops? (Boolean/parseBoolean (get-in request [:query-params "with-ops"]))
        from-tx-id (some->> (get-in request [:query-params "from-tx-id"])
                            (Long/parseLong))
        result (api/open-tx-log crux-node from-tx-id with-ops?)]
    (-> (streamed-edn-response result)
        (add-last-modified (:crux.tx/tx-time (api/latest-completed-tx crux-node))))))

(defn- sync-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        ;; TODO this'll get cut down with the rest of the sync deprecation
        transaction-time (some->> (get-in request [:query-params "transactionTime"])
                                  (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (if transaction-time
                          (api/await-tx-time crux-node transaction-time timeout)
                          (api/sync crux-node timeout))]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-time-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-time (some->> (get-in request [:query-params "tx-time"])
                         (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (api/await-tx-time crux-node tx-time timeout)]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-id (-> (get-in request [:query-params "tx-id"])
                  (Long/parseLong))]
    (let [{:keys [crux.tx/tx-time] :as tx} (api/await-tx crux-node {:crux.tx/tx-id tx-id} timeout)]
      (-> (success-response tx)
          (add-last-modified tx-time)))))

(defn- attribute-stats [^ICruxAPI crux-node]
  (success-response (api/attribute-stats crux-node)))

(defn- tx-committed? [^ICruxAPI crux-node request]
  (try
    (let [tx-id (-> (get-in request [:query-params "tx-id"])
                    (Long/parseLong))]
      (success-response (api/tx-committed? crux-node {:crux.tx/tx-id tx-id})))
    (catch NodeOutOfSyncException e
      (exception-response 400 e))))

(defn latest-completed-tx [^ICruxAPI crux-node]
  (success-response (api/latest-completed-tx crux-node)))

(defn latest-submitted-tx [^ICruxAPI crux-node]
  (success-response (api/latest-submitted-tx crux-node)))

(def ^:private sparql-available?
  (try ; you can change it back to require when clojure.core fixes it to be thread-safe
    (requiring-resolve 'crux.sparql.protocol/sparql-query)
    true
    (catch IOException _
      false)))

;; ---------------------------------------------------
;; Jetty server

(defn- handler [crux-node request]
  (condp check-path request
    [#"^/$" [:get]]
    (status crux-node)

    [#"^/document/.+$" [:get :post]]
    (document crux-node request)

    [#"^/documents" [:post]]
    (documents crux-node request)

    [#"^/entity$" [:post]]
    (entity crux-node request)

    [#"^/entity-tx$" [:post]]
    (entity-tx crux-node request)

    [#"^/history/.+$" [:get :post]]
    (history crux-node request)

    [#"^/history-range/.+$" [:get]]
    (history-range crux-node request)

    [#"^/history-ascending$" [:post]]
    (history-ascending crux-node request)

    [#"^/history-descending$" [:post]]
    (history-descending crux-node request)

    [#"^/query$" [:post]]
    (query crux-node request)

    [#"^/query-stream$" [:post]]
    (query-stream crux-node request)

    [#"^/attribute-stats" [:get]]
    (attribute-stats crux-node)

    [#"^/sync$" [:get]]
    (sync-handler crux-node request)

    [#"^/await-tx$" [:get]]
    (await-tx-handler crux-node request)

    [#"^/await-tx-time$" [:get]]
    (await-tx-time-handler crux-node request)

    [#"^/tx-log$" [:get]]
    (tx-log crux-node request)

    [#"^/tx-log$" [:post]]
    (transact crux-node request)

    [#"^/tx-committed$" [:get]]
    (tx-committed? crux-node request)

    [#"^/latest-completed-tx" [:get]]
    (latest-completed-tx crux-node)

    [#"^/latest-submitted-tx" [:get]]
    (latest-submitted-tx crux-node)

    (if (and (check-path [#"^/sparql/?$" [:get :post]] request)
             sparql-available?)
      ((resolve 'crux.sparql.protocol/sparql-query) crux-node request)
      {:status 400
       :headers {"Content-Type" "text/plain"}
       :body "Unsupported method on this address."})))

(def ^:const default-server-port 3000)

(defrecord HTTPServer [^Server server options]
  Closeable
  (close [_]
    (.stop server)))

(defn ^:deprecated start-http-server
  "Starts a HTTP server listening to the specified server-port, serving
  the Crux HTTP API. Takes a either a crux.api.ICruxAPI or its
  dependencies explicitly as arguments (internal use)."
  ^java.io.Closeable
  ([crux-node] (start-http-server crux-node {}))
  ([crux-node
    {:keys [server-port cors-access-control]
     :or {server-port default-server-port cors-access-control []}
     :as options}]
   (let [wrap-cors' #(apply wrap-cors (cons % cors-access-control))
         server (j/run-jetty (-> (partial handler crux-node)
                                 (wrap-exception-handling)
                                 (p/wrap-params)
                                 (wrap-cors')
                                 (wrap-exception-handling))
                             {:port server-port
                              :join? false})]
     (log/info "HTTP server started on port: " server-port)
     (->HTTPServer server options))))

(def module
  {::server {:start-fn (fn [{:keys [crux.node/node]} {::keys [port] :as options}]
                         (let [server (j/run-jetty (-> (partial handler node)
                                                       (p/wrap-params)
                                                       (wrap-exception-handling))
                                                   {:port port
                                                    :join? false})]
                           (log/info "HTTP server started on port: " port)
                           (->HTTPServer server options)))
             :deps #{:crux.node/node}

             ;; I'm deliberately not porting across CORS here as I don't think we should be encouraging
             ;; Crux servers to be exposed directly to browsers. Better pattern here for individual apps
             ;; to expose the functionality they need to? (JH)
             :args {::port {:crux.config/type :crux.config/nat-int
                            :doc "Port to start the HTTP server on"
                            :default default-server-port}}}})
