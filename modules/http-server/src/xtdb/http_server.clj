(ns xtdb.http-server
  "HTTP API for Crux.
  The optional SPARQL handler requires juxt.crux/rdf."
  (:require [juxt.clojars-mirrors.camel-snake-kebab.v0v4v2.camel-snake-kebab.core :as csk]
            [clojure.edn :as edn]
            [clojure.instant :as instant]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.http-server.entity :as entity]
            [xtdb.http-server.json :as http-json]
            [xtdb.http-server.query :as query]
            [xtdb.http-server.status :as status]
            [xtdb.http-server.util :as util]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.tx :as tx]
            [xtdb.tx.conform :as txc]
            [juxt.clojars-mirrors.jsonista.v0v3v1.jsonista.core :as json]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.core :as m]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc]
            [juxt.clojars-mirrors.reitit-spec.v0v5v12.reitit.coercion.spec :as reitit.spec]
            [juxt.clojars-mirrors.reitit-ring.v0v5v12.reitit.ring :as rr]
            [juxt.clojars-mirrors.reitit-ring.v0v5v12.reitit.ring.coercion :as rrc]
            [juxt.clojars-mirrors.reitit-middleware.v0v5v12.reitit.ring.middleware.exception :as re]
            [juxt.clojars-mirrors.reitit-middleware.v0v5v12.reitit.ring.middleware.muuntaja :as rm]
            [juxt.clojars-mirrors.reitit-swagger.v0v5v12.reitit.swagger :as swagger]
            [juxt.clojars-mirrors.ring-jetty-adapter.v0v14v2.ring.adapter.jetty9 :as j]
            [juxt.clojars-mirrors.ring-core.v1v9v2.ring.middleware.params :as p]
            [juxt.clojars-mirrors.ring-core.v1v9v2.ring.util.response :as resp]
            [juxt.clojars-mirrors.ring-core.v1v9v2.ring.util.time :as rt])
  (:import [com.nimbusds.jose.crypto ECDSAVerifier RSASSAVerifier]
           [com.nimbusds.jose.jwk ECKey JWKSet KeyType RSAKey]
           com.nimbusds.jwt.SignedJWT
           [xtdb.api NodeOutOfSyncException]
           [java.io Closeable IOException]
           java.time.Duration
           org.eclipse.jetty.server.Server))

(defn- add-last-modified [response date]
  (cond-> response
    date (assoc-in [:headers "Last-Modified"] (rt/format-date date))))

(s/def ::db-spec (s/keys :opt-un [::util/valid-time ::util/tx-time ::util/tx-id]))

(defn- db-handler [crux-node]
  (fn [req]
    (let [{:keys [valid-time tx-time tx-id]} (get-in req [:parameters :query])
          db (util/db-for-request crux-node {:valid-time valid-time
                                             :tx-time tx-time
                                             :tx-id tx-id})]
      (resp/response (merge {:xt/valid-time (xt/valid-time db)}
                            (xt/db-basis db))))))

(s/def ::entity-tx-spec (s/keys :req-un [(or ::util/eid-edn ::util/eid-json ::util/eid)]
                                :opt-un [::util/valid-time ::util/tx-time ::util/tx-id]))

(defn- entity-tx [crux-node]
  (fn [req]
    (let [{:keys [eid eid-edn eid-json valid-time tx-time tx-id]} (get-in req [:parameters :query])
          eid (or eid-edn eid-json eid)
          db (util/db-for-request crux-node {:valid-time valid-time
                                             :tx-time tx-time
                                             :tx-id tx-id})
          entity-tx (xt/entity-tx db eid)]
      (if entity-tx
        (-> {:status 200
             :body entity-tx}
            (add-last-modified (:xt/tx-time entity-tx)))
        {:status 404
         :body {:error (str eid " entity-tx not found")}}))))

(defn- ->submit-json-decoder [_]
  (let [decoders {::txc/->doc #(xio/update-if % :crux.db/fn edn/read-string)
                  ::txc/->valid-time (fn [vt-str]
                                       (try
                                         (instant/read-instant-date vt-str)
                                         (catch Exception _e
                                           vt-str)))}]
    (reify
      mfc/Decode
      (decode [_ data _]
        (-> (json/read-value data http-json/crux-object-mapper)
            (update :tx-ops (fn [tx-ops]
                              (->> tx-ops
                                   (mapv (fn [tx-op]
                                           (-> tx-op
                                               (update 0 (fn [op] (keyword "xt" op)))
                                               (txc/conform-tx-op decoders)
                                               (txc/->tx-op))))))))))))

(def ->submit-tx-muuntaja
  (m/create
   (assoc-in (util/->default-muuntaja {:json-encode-fn http-json/camel-case-keys})
             [:formats "application/json" :decoder]
             [->submit-json-decoder])))

(s/def ::tx-ops vector?)
(s/def ::submit-tx-spec (s/keys :req-un [::tx-ops]))

(defn- submit-tx [crux-node]
  (fn [req]
    (let [tx-ops (get-in req [:parameters :body :tx-ops])
          {:keys [xt/tx-time] :as submitted-tx} (xt/submit-tx crux-node tx-ops)]
      (-> {:status 202
           :body submitted-tx}
          (add-last-modified tx-time)))))

(s/def ::with-ops? boolean?)
(s/def ::after-tx-id int?)
(s/def ::tx-log-spec (s/keys :opt-un [::with-ops? ::after-tx-id]))

(defn txs->json [txs]
  (mapv #(update % 0 name) txs))

(defn tx-log-json-encode [tx]
  (-> tx
      (xio/update-if :xt/tx-ops txs->json)
      (xio/update-if :xtdb.tx.event/tx-events txs->json)
      (http-json/camel-case-keys)))

(def ->tx-log-muuntaja
  (m/create
   (-> (util/->default-muuntaja {:json-encode-fn tx-log-json-encode})
       (assoc :return :output-stream))))

(defn- tx-log [crux-node]
  (fn [req]
    (let [{:keys [with-ops? after-tx-id]} (get-in req [:parameters :query])]
      (-> {:status 200
           :body {:results (xt/open-tx-log crux-node after-tx-id with-ops?)}
           :return :output-stream}
          (add-last-modified (:xt/tx-time (xt/latest-completed-tx crux-node)))))))

(s/def ::sync-spec (s/keys :opt-un [::util/tx-time ::util/timeout]))

(defn- sync-handler [crux-node]
  (fn [req]
    (let [{:keys [timeout tx-time]} (get-in req [:parameters :query])
          timeout (some-> timeout (Duration/ofMillis))
          last-modified (if tx-time
                          (xt/await-tx-time crux-node tx-time timeout)
                          (xt/sync crux-node timeout))]
      (-> {:status 200
           :body {:xt/tx-time last-modified}}
          (add-last-modified last-modified)))))

(s/def ::await-tx-time-spec (s/keys :req-un [::util/tx-time] :opt-un [::util/timeout]))

(defn- await-tx-time-handler [crux-node]
  (fn [req]
    (let [{:keys [timeout tx-time]} (get-in req [:parameters :query])
          timeout (some-> timeout (Duration/ofMillis))]
      (let [last-modified (xt/await-tx-time crux-node tx-time timeout)]
        (->
         {:status 200
          :body {:xt/tx-time last-modified}}
         (add-last-modified last-modified))))))

(s/def ::await-tx-spec (s/keys :req-un [::util/tx-id] :opt-un [::util/timeout]))

(defn- await-tx-handler [crux-node]
  (fn [req]
    (let [{:keys [timeout tx-id]} (get-in req [:parameters :query])
          timeout (some-> timeout (Duration/ofMillis))
          {:keys [xt/tx-time] :as tx} (xt/await-tx crux-node {:xt/tx-id tx-id} timeout)]
      (-> {:status 200, :body tx}
          (add-last-modified tx-time)))))

(defn- attribute-stats [crux-node]
  (fn [_]
    {:status 200
     :body (xt/attribute-stats crux-node)}))

(s/def ::tx-committed-spec (s/keys :req-un [::util/tx-id]))

(defn- tx-committed? [crux-node]
  (fn [req]
    (try
      (let [tx-id (get-in req [:parameters :query :tx-id])]
        {:status 200
         :body {:tx-committed? (xt/tx-committed? crux-node {:xt/tx-id tx-id})}})
      (catch NodeOutOfSyncException e
        {:status 400, :body e}))))

(defn latest-completed-tx [crux-node]
  (fn [_]
    (if-let [latest-completed-tx (xt/latest-completed-tx crux-node)]
      {:status 200
       :body latest-completed-tx}
      {:status 404
       :body {:error "No latest-completed-tx found."}})))

(defn latest-submitted-tx [crux-node]
  (fn [_]
    (if-let [latest-submitted-tx (xt/latest-submitted-tx crux-node)]
      {:status 200
       :body latest-submitted-tx}
      {:status 404
       :body {:error "No latest-submitted-tx found."}})))

(defn active-queries [crux-node]
  (fn [_]
    {:status 200
     :body (xt/active-queries crux-node)}))

(defn recent-queries [crux-node]
  (fn [_]
    {:status 200
     :body (xt/recent-queries crux-node)}))

(defn slowest-queries [crux-node]
  (fn [_]
    {:status 200
     :body (xt/slowest-queries crux-node)}))

(def ^:private sparql-available?
  (try ; you can change it back to require when clojure.core fixes it to be thread-safe
    (requiring-resolve 'xtdb.sparql.protocol/sparql-query)
    true
    (catch IOException _
      false)))

(defn sparqql [crux-node]
  (fn [req]
    (when sparql-available?
      ((resolve 'xtdb.sparql.protocol/sparql-query) crux-node req))))

(defn- add-response-format [handler format]
  (fn [req]
    (-> (handler (assoc-in req [:muuntaja/response :format] format))
        (assoc :muuntaja/content-type format))))

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

(defn handle-ex-info [ex req]
  {:status 400
   :body (ex-data ex)})

(defn handle-muuntaja-decode-error [ex req]
  {:status 400
   :body {:error (str "Malformed " (-> ex ex-data :format pr-str) " request.") }})

(defn wrap-camel-case-params [handler]
  (fn [{:keys [query-params] :as request}]
    (let [kebab-qps (into {} (map (fn [[k v]] [(csk/->kebab-case k) v])) query-params)]
      (handler (assoc request :query-params kebab-qps)))))

(defn- query-list-json-encode [query-states]
  (map (fn [qs]
         (-> qs
             (update :query pr-str)
             http-json/camel-case-keys))
       query-states))

(def ^:private query-list-muuntaja
  (m/create (util/->default-muuntaja {:json-encode-fn query-list-json-encode})))

(def default-muuntaja
  (m/create (util/->default-muuntaja {:json-encode-fn http-json/camel-case-keys})))

(defn- make-cursors [example]
  (let [example-meta (meta example)]
    (cond
      (:results-cursor example-meta) {:results (xio/->cursor #() example)}
      :else example)))

(defn- with-example [{:keys [muuntaja] :or {muuntaja default-muuntaja} :as handler} example-filename]
  (let [example (-> (io/resource (format "xtdb/http_server/examples/%s.edn" example-filename))
                    slurp
                    read-string)]
    (-> handler
        (assoc-in [:responses 200]
                  {:examples
                   {"application/json" (-> (m/encode muuntaja "application/json" (make-cursors example))
                                           (m/slurp)
                                           (json/read-value))
                    "application/edn" (with-out-str (pp/pprint example))
                    "application/transit+json" (-> (m/encode muuntaja "application/transit+json" (make-cursors example))
                                                   (m/slurp)
                                                   (json/read-value))}}))))

(defn- ->crux-router [{{:keys [^String jwks, read-only?]} :http-options
                       :keys [crux-node], :as opts}]
  (let [opts (-> opts (update :http-options dissoc :jwks))
        query-handler {:muuntaja (query/->query-muuntaja opts)
                       :summary "Query"
                       :description "Perform a datalog query"
                       :get {:handler (query/data-browser-query opts)
                             :parameters {:query ::query/query-params}}
                       :post {:handler (query/data-browser-query opts)
                              :parameters {:query ::query/query-params
                                           :body ::query/body-params}}}]
    (rr/router [["/" {:no-doc true
                      :get (fn [_] (resp/redirect "/_xtdb/query"))}]
                ["/_xtdb"
                 ["/db" (-> {:get (db-handler crux-node)
                             :parameters {:query ::db-spec}
                             :summary "DB"
                             :description "Get the resolved db-basis for the given valid-time/transactoin"}
                            (with-example "db-response"))]
                 ["/status" (-> {:muuntaja (status/->status-muuntaja opts)
                                 :summary "Status"
                                 :description "Get status information from the node"
                                 :get (status/status opts)}
                                (with-example "status-response"))]
                 ["/entity" (-> {:muuntaja (entity/->entity-muuntaja opts)
                                 :summary "Entity"
                                 :description "Get information about a particular entity"
                                 :get (entity/entity-state opts)
                                 :parameters {:query ::entity/query-params}}
                                (with-example "entity-response"))]
                 ["/query" (-> query-handler
                               (with-example "query-response"))]
                 ["/query.csv" (assoc query-handler :middleware [[add-response-format "text/csv"]] :no-doc true)]
                 ["/query.tsv" (assoc query-handler :middleware [[add-response-format "text/tsv"]] :no-doc true)]
                 ["/entity-tx" (-> {:get (entity-tx crux-node)
                                    :summary "Entity Tx"
                                    :description "Get transactional information an particular entity"
                                    :parameters {:query ::entity-tx-spec}}
                                   (with-example "entity-tx-response"))]
                 ["/attribute-stats" (-> {:get (attribute-stats crux-node)
                                          :summary "Attribute Stats"
                                          :description "Get frequencies of indexed attributes"
                                          :muuntaja (m/create (util/->default-muuntaja {:json-encode-fn identity}))}
                                         (with-example "attribute-stats-response"))]
                 ["/sync" (-> {:get (sync-handler crux-node)
                               :summary "Sync"
                               :description "Wait until the Kafka consumer’s lag is back to 0"
                               :parameters {:query ::sync-spec}}
                              (with-example "sync-response"))]
                 ["/await-tx" (-> {:get (await-tx-handler crux-node)
                                   :summary "Await Tx"
                                   :description "Wait until the node has indexed a transaction at or past the supplied tx-id"
                                   :parameters {:query ::await-tx-spec}}
                                  (with-example "await-tx-response"))]
                 ["/await-tx-time" (-> {:get (await-tx-time-handler crux-node)
                                        :summary "Await Tx Time"
                                        :description "Wait until the node has indexed a transaction that is past the supplied tx-time"
                                        :parameters {:query ::await-tx-time-spec}}
                                       (with-example "await-tx-time-response"))]
                 ["/tx-log" (-> {:get (tx-log crux-node)
                                 :summary "Tx Log"
                                 :description "Get a list of all transactions"
                                 :muuntaja ->tx-log-muuntaja
                                 :parameters {:query ::tx-log-spec}}
                                (with-example "tx-log-response"))]
                 ["/submit-tx" (-> {:muuntaja ->submit-tx-muuntaja
                                    :summary "Submit Tx"
                                    :description "Takes a vector of transactions - Writes to the node"
                                    :post (if read-only?
                                            (fn [_] {:status 403
                                                     :body "forbidden: read-only HTTP node"})
                                            (submit-tx crux-node))
                                    :parameters {:body ::submit-tx-spec}}
                                   (with-example "submit-tx-response"))]
                 ["/tx-committed" (-> {:get (tx-committed? crux-node)
                                       :summary "Tx Committed"
                                       :description "Checks if a submitted tx was successfully committed"
                                       :parameters {:query ::tx-committed-spec}}
                                      (with-example "tx-committed-response"))]
                 ["/latest-completed-tx" (-> {:get (latest-completed-tx crux-node)
                                              :summary "Latest Completed Tx"
                                              :description "Get the latest transaction to have been indexed by this node"}
                                             (with-example "latest-completed-tx-response"))]
                 ["/latest-submitted-tx" (-> {:get (latest-submitted-tx crux-node)
                                              :summary "Latest Submitted Tx"
                                              :description "Get the latest transaction to have been submitted to this cluster"}
                                             (with-example "latest-submitted-tx-response"))]
                 ["/active-queries" (-> {:get (active-queries crux-node)
                                         :summary "Active Queries"
                                         :description "Get a list of currently running queries"
                                         :muuntaja query-list-muuntaja}
                                        (with-example "active-queries-response"))]
                 ["/recent-queries" (-> {:get (recent-queries crux-node)
                                         :summary "Recent Queries"
                                         :description "Get a list of recently completed/failed queries"
                                         :muuntaja query-list-muuntaja}
                                        (with-example "recent-queries-response"))]
                 ["/slowest-queries" (-> {:get (slowest-queries crux-node)
                                          :summary "Slowest Queries"
                                          :description "Get a list of slowest completed/failed queries ran on the node"
                                          :muuntaja query-list-muuntaja}
                                         (with-example "slowest-queries-response"))]
                 ["/sparql" {:get (sparqql crux-node)
                             :post (sparqql crux-node)
                             :no-doc true}]

                 ["/swagger.json"
                  {:get {:no-doc true
                         :swagger {:info {:title "Crux API"}}
                         :handler (swagger/create-swagger-handler)
                         :muuntaja (m/create (assoc (util/->default-muuntaja {}) :default-format "application/json"))}}]]]

               {:data
                {:muuntaja default-muuntaja
                 :coercion reitit.spec/coercion
                 :middleware (cond-> [p/wrap-params
                                      wrap-camel-case-params
                                      rm/format-negotiate-middleware
                                      rm/format-response-middleware
                                      (re/create-exception-middleware
                                       (merge re/default-handlers
                                              {xtdb.IllegalArgumentException handle-ex-info
                                               xtdb.api.NodeOutOfSyncException handle-ex-info
                                               :muuntaja/decode handle-muuntaja-decode-error}))
                                      rm/format-request-middleware
                                      rrc/coerce-response-middleware
                                      rrc/coerce-request-middleware]
                               jwks (conj #(wrap-jwt % (JWKSet/parse jwks))))}})))

;; entry point for users including our handler in their own server
(defn ->crux-handler [crux-node http-options]
  (rr/routes
   (rr/ring-handler (->crux-router {:crux-node crux-node
                                    :http-options http-options}))
   (rr/create-resource-handler {:path "/"})))

(alter-meta! #'->crux-handler assoc :arglists '([crux-node {:keys [jwks read-only? server-label]}]))

(defn ->server {::sys/deps {:crux-node :xtdb/node}
                ::sys/args {:port {:spec ::sys/nat-int
                                   :doc "Port to start the HTTP server on"
                                   :default default-server-port}
                            :read-only? {:spec ::sys/boolean
                                         :doc "Whether to start the Crux HTTP server in read-only mode"
                                         :default false}
                            :jwks {:spec ::sys/string
                                   :doc "JWKS string to validate against"}
                            :server-label {:spec ::sys/string}
                            :jetty-opts {:doc "Extra options to pass to Jetty, see https://ring-clojure.github.io/ring/ring.adapter.jetty.html"}}}
  [{:keys [crux-node port jetty-opts] :as options}]
  (let [server (j/run-jetty (rr/ring-handler (->crux-router {:crux-node crux-node
                                                             :http-options (dissoc options :crux-node :port :jetty-opts)})
                                             (rr/routes
                                              (rr/create-resource-handler {:path "/"})
                                              (rr/create-default-handler)))
                            (merge {:port port
                                    :h2c? true
                                    :h2? true
                                    :join? false}
                                   jetty-opts))]
    (log/info "HTTP server started on port: " port)
    (->HTTPServer server options)))
