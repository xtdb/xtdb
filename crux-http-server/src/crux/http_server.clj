(ns crux.http-server
  "HTTP API for Crux.
  The optional SPARQL handler requires juxt.crux/rdf."
  (:require [clojure.instant :as instant]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.http-server.query :as query]
            [crux.http-server.entity :as entity]
            [crux.http-server.status :as status]
            [crux.http-server.util :as util]
            [crux.io :as cio]
            [crux.system :as sys]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mfc]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.util.io :as rio]
            [ring.util.request :as req]
            [ring.util.response :as resp]
            [ring.util.time :as rt])
  (:import [com.nimbusds.jose.crypto ECDSAVerifier RSASSAVerifier]
           [com.nimbusds.jose.jwk ECKey JWKSet KeyType RSAKey]
           com.nimbusds.jwt.SignedJWT
           [crux.api ICruxAPI NodeOutOfSyncException]
           [java.io Closeable IOException]
           [java.time Duration]
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

(defn- redirect-response [url]
  {:status 302
   :headers {"Location" url}})

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
          (if (or (instance? IllegalArgumentException e)
                  (and (.getMessage e)
                       (str/starts-with? (.getMessage e) "Spec assertion failed")))
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


(defn- streamed-edn-response [^Closeable ctx edn]
  (try
    (->> (rio/piped-input-stream
          (fn [out]
            (with-open [ctx ctx
                        out (io/writer out)]
              (.write out "(")
              (doseq [x edn]
                (.write out (cio/pr-edn-str x)))
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

(defn- validate-or-throw [body-edn spec]
  (when-not (s/valid? spec body-edn)
    (throw (ex-info (str "Spec assertion failed\n" (s/explain-str spec body-edn)) (s/explain-data spec body-edn)))))

(defn- query [^ICruxAPI crux-node request]
  (let [query-map (doto (body->edn request) (validate-or-throw ::query-map))
        db (util/db-for-request crux-node query-map)
        result (api/open-q db (:query query-map))]
    (-> (streamed-edn-response result (iterator-seq result))
        (add-last-modified (.transactionTime db)))))

(s/def ::eid c/valid-id?)
(s/def ::entity-map (s/and (s/keys :opt-un [::valid-time ::transact-time])))

(defn- entity [^ICruxAPI crux-node {:keys [query-params] :as request}]
  (let [body (doto (body->edn request) (validate-or-throw ::entity-map))
        eid (or (:eid body)
                (some-> (re-find #"^/entity/(.+)$" (req/path-info request))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))
        db (util/db-for-request crux-node {:valid-time (or (:valid-time body)
                                                           (some-> (get query-params "valid-time")
                                                                   (instant/read-instant-date)))
                                      :transact-time (or (:transact-time body)
                                                         (some-> (get query-params "transaction-time")
                                                                 (instant/read-instant-date)))})
        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response (.entity db eid))
        (add-last-modified tx-time))))

(defn- entity-tx [^ICruxAPI crux-node {:keys [query-params] :as request}]
  (let [body (doto (body->edn request) (validate-or-throw ::entity-map))
        eid (or (:eid body)
                (some-> (re-find #"^/entity-tx/(.+)$" (req/path-info request))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))

        db (util/db-for-request crux-node {:valid-time (or (:valid-time body)
                                                           (some-> (get query-params "valid-time")
                                                                   (instant/read-instant-date)))
                                           :transact-time (or (:transact-time body)
                                                              (some-> (get query-params "transaction-time")
                                                                      (instant/read-instant-date)))})

        {:keys [crux.tx/tx-time] :as entity-tx} (.entityTx db eid)]
    (-> (success-response entity-tx)
        (add-last-modified tx-time))))

(defn- entity-history [^ICruxAPI node
                       {{:strs [sort-order
                                valid-time transaction-time
                                start-valid-time start-transaction-time
                                end-valid-time end-transaction-time
                                with-corrections with-docs]} :query-params
                        :as req}]
  (let [db (util/db-for-request node {:valid-time (some-> valid-time (instant/read-instant-date))
                                 :transact-time (some-> transaction-time (instant/read-instant-date))})
        eid (or (some-> (re-find #"^/entity-history/(.+)$" (req/path-info req))
                        second
                        c/id-edn-reader)
                (throw (IllegalArgumentException. "missing eid")))
        sort-order (or (some-> sort-order keyword)
                       (throw (IllegalArgumentException. "missing sort-order query parameter")))
        opts {:with-corrections? (some-> ^String with-corrections Boolean/valueOf)
              :with-docs? (some-> ^String with-docs Boolean/valueOf)
              :start {:crux.db/valid-time (some-> start-valid-time (instant/read-instant-date))
                      :crux.tx/tx-time (some-> start-transaction-time (instant/read-instant-date))}
              :end {:crux.db/valid-time (some-> end-valid-time (instant/read-instant-date))
                    :crux.tx/tx-time (some-> end-transaction-time (instant/read-instant-date))}}
        history (api/open-entity-history db eid sort-order opts)]
    (-> (streamed-edn-response history (iterator-seq history))
        (add-last-modified (:crux.tx/tx-time (api/latest-completed-tx node))))))

(defn- transact [^ICruxAPI crux-node request]
  (let [tx-ops (body->edn request)
        {:keys [crux.tx/tx-time] :as submitted-tx} (.submitTx crux-node tx-ops)]
    (-> (success-response submitted-tx)
        (assoc :status 202)
        (add-last-modified tx-time))))

;; TODO: Could add from date parameter.
(defn- tx-log [^ICruxAPI crux-node request]
  (let [with-ops? (Boolean/parseBoolean (get-in request [:query-params "with-ops"]))
        after-tx-id (some->> (get-in request [:query-params "after-tx-id"])
                             (Long/parseLong))
        result (.openTxLog crux-node after-tx-id with-ops?)]
    (-> (streamed-edn-response result (iterator-seq result))
        (add-last-modified (:crux.tx/tx-time (.latestCompletedTx crux-node))))))

(defn- sync-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        ;; TODO this'll get cut down with the rest of the sync deprecation
        transaction-time (some->> (get-in request [:query-params "transactionTime"])
                                  (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (if transaction-time
                          (.awaitTxTime crux-node transaction-time timeout)
                          (.sync crux-node timeout))]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-time-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-time (some->> (get-in request [:query-params "tx-time"])
                         (cio/parse-rfc3339-or-millis-date))]
    (let [last-modified (.awaitTxTime crux-node tx-time timeout)]
      (-> (success-response last-modified)
          (add-last-modified last-modified)))))

(defn- await-tx-handler [^ICruxAPI crux-node request]
  (let [timeout (some->> (get-in request [:query-params "timeout"])
                         (Long/parseLong)
                         (Duration/ofMillis))
        tx-id (-> (get-in request [:query-params "tx-id"])
                  (Long/parseLong))]
    (let [{:keys [crux.tx/tx-time] :as tx} (.awaitTx crux-node {:crux.tx/tx-id tx-id} timeout)]
      (-> (success-response tx)
          (add-last-modified tx-time)))))

(defn- attribute-stats [^ICruxAPI crux-node]
  (success-response (.attributeStats crux-node)))

(defn- tx-committed? [^ICruxAPI crux-node request]
  (try
    (let [tx-id (-> (get-in request [:query-params "tx-id"])
                    (Long/parseLong))]
      (success-response (.hasTxCommitted crux-node {:crux.tx/tx-id tx-id})))
    (catch NodeOutOfSyncException e
      (exception-response 400 e))))

(defn latest-completed-tx [^ICruxAPI crux-node]
  (success-response (.latestCompletedTx crux-node)))

(defn latest-submitted-tx [^ICruxAPI crux-node]
  (success-response (.latestSubmittedTx crux-node)))

(defn active-queries [^ICruxAPI crux-node]
  (success-response (api/active-queries crux-node)))

(defn recent-queries [^ICruxAPI crux-node]
  (success-response (api/recent-queries crux-node)))

(defn slowest-queries [^ICruxAPI crux-node]
  (success-response (api/slowest-queries crux-node)))

(def ^:private sparql-available?
  (try ; you can change it back to require when clojure.core fixes it to be thread-safe
    (requiring-resolve 'crux.sparql.protocol/sparql-query)
    true
    (catch IOException _
      false)))

;; ---------------------------------------------------
;; Jetty server

(defn- handler [request {:keys [crux-node read-only?]}]
  (condp check-path request
    [#"^/$" [:get]]
    (redirect-response "/_crux/query")

    [#"^/entity/.+$" [:get]]
    (entity crux-node request)

    [#"^/entity-tx/.+$" [:get]]
    (entity-tx crux-node request)

    [#"^/entity$" [:post]]
    (entity crux-node request)

    [#"^/entity-tx$" [:post]]
    (entity-tx crux-node request)

    [#"^/entity-history/.+$" [:get]]
    (entity-history crux-node request)

    [#"^/query$" [:post]]
    (query crux-node request)

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
    (if read-only?
      (-> (resp/response "forbidden: read-only HTTP node")
          (resp/status 403))
      (transact crux-node request))

    [#"^/tx-committed$" [:get]]
    (tx-committed? crux-node request)

    [#"^/latest-completed-tx" [:get]]
    (latest-completed-tx crux-node)

    [#"^/latest-submitted-tx" [:get]]
    (latest-submitted-tx crux-node)

    [#"^/active-queries" [:get]]
    (active-queries crux-node)

    [#"^/recent-queries" [:get]]
    (recent-queries crux-node)

    [#"^/slowest-queries" [:get]]
    (slowest-queries crux-node)

    (if (and (check-path [#"^/sparql/?$" [:get :post]] request)
             sparql-available?)
      ((resolve 'crux.sparql.protocol/sparql-query) crux-node request)
      nil)))

(defn- ->data-browser-handler [{:keys [crux-node] :as options}]
  (let [options (assoc options
                       :query-muuntaja (query/->query-muuntaja options)
                       :entity-muuntaja (entity/->entity-muuntaja options)
                       :status-muuntaja (status/->status-muuntaja options))]
    (fn [request]
      (condp check-path request
        [#"^/_crux/status" [:get]]
        (status/status request options)

        [#"^/_crux/entity$" [:get]]
        (entity/entity-state request options)

        [#"^/_crux/query$" [:get]]
        (query/data-browser-query request options)

        [#"^/_crux/query.csv$" [:get]]
        (query/data-browser-query (assoc-in request [:muuntaja/response :format] "text/csv") options)

        [#"^/_crux/query.tsv$" [:get]]
        (query/data-browser-query (assoc-in request [:muuntaja/response :format] "text/tsv") options)

        nil))))

(def ^:const default-server-port 3000)

(defrecord HTTPServer [^Server server options]
  Closeable
  (close [_]
    (.stop server)))

(defn valid-jwt?
  "Return true if the given JWS is valid with respect to the given
  signing key."
  [^String jwt ^JWKSet jwks]
  (try
    (let [jws (SignedJWT/parse ^String jwt)
          kid (.. jws getHeader getKeyID)
          jwk (.getKeyByKeyId jwks kid)
          verifier (case (.getValue ^KeyType (.getKeyType jwk))
                     "RSA" (RSASSAVerifier. ^RSAKey jwk)
                     "EC"  (ECDSAVerifier. ^ECKey jwk))]
      (.verify jws verifier))
    (catch Exception _
      false)))

(defn wrap-jwt [handler jwks]
  (fn [request]
    (if-not (valid-jwt? (or (get-in request [:headers "x-amzn-oidc-accesstoken"])
                            (some->> (get-in request [:headers "authorization"])
                                     (re-matches #"Bearer (.*)")
                                     (second)))
                        jwks)
      {:status 401
       :body "JWT Failed to validate"}

      (handler request))))

(defn ->server {::sys/deps {:crux-node :crux/node}
                ::sys/args {:port {:spec ::sys/nat-int
                                   :doc "Port to start the HTTP server on"
                                   :default default-server-port}
                            :read-only? {:spec ::sys/boolean
                                         :doc "Whether to start the Crux HTTP server in read-only mode"
                                         :default false}
                            :jwks {:spec ::sys/string
                                   :doc "JWKS string to validate against"}
                            :server-label {:spec ::sys/string}}}
  [{:keys [crux-node port ^String jwks] :as options}]
  (let [server (j/run-jetty
                (-> (some-fn #(handler % (dissoc options :jwks))
                             (->data-browser-handler {:crux-node crux-node
                                                      :node-options (dissoc options :crux-node :jwks)})
                             (fn [_request]
                               {:status 404
                                :headers {"Content-Type" "text/plain"}
                                :body "Could not find resource."}))
                    (p/wrap-params)
                    (wrap-resource "public")
                    (wrap-exception-handling)
                    (cond-> jwks (wrap-jwt (JWKSet/parse jwks))))
                {:port port
                 :join? false})]
    (log/info "HTTP server started on port: " port)
    (->HTTPServer server options)))
