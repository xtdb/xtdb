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
            [ring.util.time :as rt])
  (:import [crux.api ICruxAPI ICruxDatasource]
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
  (response (if m
              200
              404)
            {"Content-Type" "application/edn"}
            (pr-str m)))

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
            (do (log/error e "Exception while handling request:" (pr-str request))
                (exception-response 500 e))))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

(defn- add-last-modified [response date]
  (if date
    (->> (rt/format-date date)
         (assoc-in response [:headers "Last-Modified"]))
    response))

;; ---------------------------------------------------
;; Services

(defn- status [^ICruxAPI crux-node]
  (let [status-map (.status crux-node)]
    (if (or (not (contains? status-map :crux.zk/zk-active?))
            (:crux.zk/zk-active? status-map))
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn- document [^ICruxAPI crux-node request]
  (let [[_ content-hash] (re-find #"^/document/(.+)$" (req/path-info request))]
    (success-response
     (.document crux-node (c/new-id content-hash)))))

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
      (cond-> (.documents crux-node ids-set)
              (not preserve-ids?) stringify-keys))))

(defn- history [^ICruxAPI crux-node request]
  (let [[_ eid] (re-find #"^/history/(.+)$" (req/path-info request))
        history (.history crux-node (c/new-id eid))]
    (-> (success-response history)
        (add-last-modified (:crux.tx/tx-time (first history))))))

(defn- parse-history-range-params [{:keys [query-params] :as request}]
  (let [[_ eid] (re-find #"^/history-range/(.+)$" (req/path-info request))
        times (map #(some-> (get query-params %) not-empty cio/parse-rfc3339-or-millis-date)
                   ["valid-time-start" "transaction-time-start" "valid-time-end" "transaction-time-end"])]
    (cons eid times)))

(defn- history-range [^ICruxAPI crux-node request]
  (let [[eid valid-time-start transaction-time-start valid-time-end transaction-time-end] (parse-history-range-params request)
        history (.historyRange crux-node (c/new-id eid) valid-time-start transaction-time-start valid-time-end transaction-time-end)
        last-modified (:crux.tx/tx-time (last history))]
    (-> (success-response history)
        (add-last-modified (:crux.tx/tx-time (last history))))))

(defn- db-for-request ^ICruxDatasource [^ICruxAPI crux-node {:keys [valid-time transact-time]}]
  (cond
    (and valid-time transact-time)
    (.db crux-node valid-time transact-time)

    valid-time
    (.db crux-node valid-time)

    ;; TODO: This could also be an error, depending how you see it,
    ;; not supported via the Java API itself.
    transact-time
    (.db crux-node (cio/next-monotonic-date) transact-time)

    :else
    (.db crux-node)))

(defn- streamed-edn-response [^Closeable ctx edn]
  (try
    (->> (rio/piped-input-stream
          (fn [out]
            (with-open [ctx ctx
                        out (io/writer out)]
              (.write out "(")
              (doseq [x edn]
                (.write out (pr-str x)))
              (.write out ")"))))
         (response 200 {"Content-Type" "application/edn"}))
    (catch Throwable t
      (.close ctx)
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
    (-> (success-response
         (.q db (:query query-map)))
        (add-last-modified (.transactionTime db)))))

(defn- query-stream [^ICruxAPI crux-node request]
  (let [query-map (s/assert ::query-map (body->edn request))
        db (db-for-request crux-node query-map)
        snapshot (.newSnapshot db)
        result (.q db snapshot (:query query-map))]
    (-> (streamed-edn-response snapshot result)
        (add-last-modified (.transactionTime db)))))

(s/def ::eid c/valid-id?)
(s/def ::entity-map (s/and #(set/superset? #{:eid :valid-time :transact-time} (keys %))
                           (s/keys :req-un [::eid]
                                   :opt-un [::valid-time
                                            ::transact-time])))

;; TODO: Could support as-of now via path and GET.
(defn- entity [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response (.entity db eid))
        (add-last-modified tx-time))))

(defn- entity-tx [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response entity-tx)
        (add-last-modified tx-time))))

(defn- history-ascending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        snapshot (.newSnapshot db)
        history (.historyAscending db snapshot (c/new-id eid))]
    (-> (streamed-edn-response snapshot history)
        (add-last-modified (tx/latest-completed-tx-time (:crux.tx-log/consumer-state (.status crux-node)))))))

(defn- history-descending [^ICruxAPI crux-node request]
  (let [{:keys [eid] :as body} (s/assert ::entity-map (body->edn request))
        db (db-for-request crux-node body)
        snapshot (.newSnapshot db)
        history (.historyDescending db snapshot (c/new-id eid))]
    (-> (streamed-edn-response snapshot history)
        (add-last-modified (tx/latest-completed-tx-time (:crux.tx-log/consumer-state (.status crux-node)))))))

(defn- transact [^ICruxAPI crux-node request]
  (let [tx-ops (body->edn request)
        {:keys [crux.tx/tx-time] :as submitted-tx} (.submitTx crux-node tx-ops)]
    (-> (success-response submitted-tx)
        (assoc :status 202)
        (add-last-modified tx-time))))

;; TODO: Could add from date parameter.
(defn- tx-log [^ICruxAPI crux-node request]
  (let [with-documents? (Boolean/parseBoolean (get-in request [:query-params "with-documents"]))
        from-tx-id (some->> (get-in request [:query-params "from-tx-id"])
                            (Long/parseLong))
        ctx (.newTxLogContext crux-node)
        result (.txLog crux-node ctx from-tx-id with-documents?)]
    (-> (streamed-edn-response ctx result)
        (add-last-modified (tx/latest-completed-tx-time (:crux.tx-log/consumer-state (.status crux-node)))))))

(defn- sync-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        transaction-time (some->> (get-in request [:query-params "transactionTime"])
                                  (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (if transaction-time
                          (.sync crux-node transaction-time timeout)
                          (.sync crux-node timeout))]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- attribute-stats [^ICruxAPI crux-node]
  (success-response (.attributeStats crux-node)))

(def ^:private sparql-available? (try ; you can change it back to require when clojure.core fixes it to be thread-safe
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

    [#"^/tx-log$" [:get]]
    (tx-log crux-node request)

    [#"^/tx-log$" [:post]]
    (transact crux-node request)

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

(defn start-http-server
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
