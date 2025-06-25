(ns xtdb.server
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mf]
            [reitit.coercion :as r.coercion]
            [reitit.coercion.spec :as rc.spec]
            [reitit.core :as r]
            [reitit.http :as http]
            [reitit.http.coercion :as rh.coercion]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.http.interceptors.muuntaja :as ri.muuntaja]
            [reitit.http.interceptors.parameters :as ri.parameters]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.ring :as r.ring]
            [reitit.swagger :as r.swagger]
            [ring.adapter.jetty9 :as j]
            [ring.util.response :as ring-response]
            [spec-tools.core :as st]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (java.io InputStream OutputStream)
           [java.net InetAddress]
           [java.nio.charset StandardCharsets]
           (java.time Duration ZoneId)
           (java.util Base64 Base64$Decoder List Map)
           [java.util.stream Stream]
           org.eclipse.jetty.server.Server
           (xtdb JsonSerde)
           (xtdb.api HttpServer$Factory TransactionKey Xtdb Xtdb$Config)
           xtdb.api.module.XtdbModule
           (xtdb.api.query IKeyFn Query)
           xtdb.error.Anomaly
           (xtdb.http QueryOptions QueryRequest TxOptions TxRequest)))

(defprotocol HttpServer
  (-http-port [http-server]))

(def json-format
  (mf/map->Format
   {:name "application/json"
    :decoder [(fn decoder [_opts]
                (reify
                  mf/Decode
                  (decode [_ data _charset] ; TODO charset
                    (if (string? data)
                      (JsonSerde/decode ^String data)
                      (JsonSerde/decode ^InputStream data)))))]

    :encoder [(fn encoder [_opts]
                (reify
                  mf/EncodeToBytes
                  (encode-to-bytes [_ data charset]
                    (.getBytes ^String (JsonSerde/encode data) ^String charset))
                  mf/EncodeToOutputStream
                  (encode-to-output-stream [_ data _charset] ; TODO charset
                    (fn [^OutputStream output-stream]
                      (JsonSerde/encode data output-stream)))))]}))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/transit+json"})
      (assoc-in [:formats "application/json"] json-format)
      (assoc :default-format "application/json")
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers] serde/transit-read-handler-map)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers] serde/transit-write-handler-map)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops seqable?)

(s/def ::key-fn (s/nilable #(instance? IKeyFn %)))

(s/def ::default-tz #(instance? ZoneId %))
(s/def ::explain? boolean?)

(s/def ::system-time ::time/datetime-value)

(s/def :xtdb.server.tx/opts (s/nilable (s/keys :opt-un [::system-time ::default-tz ::key-fn ::explain?])))

(defmethod route-handler ::default [_]
  {:get {:handler (fn [_req]
                    {:status 404
                     :body {:error "Not found"}})}
   :post {:handler (fn [_req]
                     {:status 404
                      :body {:error "Not found"}})}})

(def ^:private json-tx-encoder
  (reify
    mf/EncodeToBytes
    (encode-to-bytes [_ data charset]
      (if (instance? TransactionKey data)
        (.getBytes (JsonSerde/encode data TransactionKey) ^String charset)
        (.getBytes (JsonSerde/encode data) ^String charset)))
    mf/EncodeToOutputStream))

(defn- <-TxOptions [^TxOptions opts]
  (->> {:system-time (.getSystemTime opts)
        :default-tz (.getDefaultTz opts)}
       (into {} (remove (comp nil? val)))))

(def ^:private json-tx-decoder
  (reify
    mf/Decode
    (decode [_ data _]
      (with-open [^InputStream data data]
        (let [^TxRequest tx (JsonSerde/decode data TxRequest)]
          {:tx-ops (.getTxOps tx)
           :opts (some-> (.getOpts tx) <-TxOptions)})))))

(defmethod route-handler :tx [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc-in [:formats "application/json" :encoder] json-tx-encoder)
                           (assoc-in [:formats "application/json" :decoder] json-tx-decoder)))

   :post {:handler (fn [{:keys [node] :as req}]
                     (let [{:keys [tx-ops opts await-tx?]} (get-in req [:parameters :body])]
                       {:status 200
                        :body (if await-tx?
                                (xtp/execute-tx node tx-ops opts)
                                (xtp/submit-tx node tx-ops opts))}))

          :parameters {:body (s/keys :req-un [::tx-ops]
                                     :opt-un [:xtdb.server.tx/opts ::await-tx?])}}})

(defn- throwable->ex-info [^Throwable t]
  (ex-info (.getMessage t) {::err/error-type :unknown-runtime-error
                            :class (.getName (.getClass t))
                            :stringified (.toString t)}))

(defn- ->tj-resultset-encoder [opts]
  (reify
    mf/EncodeToBytes
    ;; we're required to be a sub-type of ETB but don't need to implement its fn.

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (if-not (ex-data res)
          (with-open [^Stream res res
                      out out]
            (let [writer (transit/writer out :json opts)]
              (try
                (.forEach res
                          (fn [el]
                            (transit/write writer el)))
                (catch Anomaly e
                  (transit/write writer e))
                (catch Throwable t
                  (transit/write writer (throwable->ex-info t))))))
          (with-open [out out]
            (let [writer (transit/writer out :json opts)]
              (transit/write writer res))))))))

(def ^:private ascii-newline (int \newline))

(defn- ->jsonl-resultset-encoder [_opts]
  (reify
    mf/EncodeToBytes

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (if-not (ex-data res)
          (with-open [^Stream res res]
            (try
              (.forEach res
                        (fn [el]
                          (JsonSerde/encode el out)
                          (.write out ^byte ascii-newline)))
              (catch Throwable t
                (JsonSerde/encode t out)
                (.write out ^byte ascii-newline))
              (finally
                (util/close res)
                (util/close out))))

          (try
            (JsonSerde/encode res out)
            (finally
              (util/close out))))))))

(defn- ->json-resultset-encoder [_opts]
  (reify
    mf/EncodeToBytes

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (if-not (ex-data res)
          (with-open [^Stream res res]
            (try
              (JsonSerde/encode (.toList res) out)

              (catch Throwable t
                (JsonSerde/encode t out)
                (.write out ^byte ascii-newline))
              (finally
                (util/close res)
                (util/close out))))

          (try
            (JsonSerde/encode res out)
            (finally
              (util/close out))))))))

(s/def ::current-time inst?)
(s/def ::snapshot-time (s/nilable ::time/datetime-value))
(s/def ::after-tx-id (s/nilable int?))

(s/def ::tx-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::query (some-fn string? seq? #(instance? Query %)))

(s/def ::args (s/nilable (some-fn #(instance? Map %)
                                  #(instance? List %))))

(s/def ::query-body
  (s/keys :req-un [::query],
          :opt-un [::after-tx-id ::snapshot-time ::current-time ::tx-timeout ::args ::default-tz ::key-fn ::explain?]))

(defn- <-QueryOpts [^QueryOptions opts]
  (->> {:args (.getArgs opts)
        :after-tx-id (.getAfterTxId opts)
        :snapshot-time (.getSnapshotTime opts)
        :current-time (.getCurrentTime opts)
        :tx-timeout (.getTxTimeout opts)
        :default-tz (.getDefaultTz opts)
        :explain? (.getExplain opts)
        :key-fn (.getKeyFn opts)}
       (into {} (remove (comp nil? val)))))

(defn json-query-decoder []
  (reify
    mf/Decode
    (decode [_ data _]
      (with-open [^InputStream data data]
        (let [^QueryRequest query-request (JsonSerde/decode data QueryRequest)]
          (-> (some-> (.queryOpts query-request) <-QueryOpts)
              (assoc :query (.sql query-request))))))))

(defmethod route-handler :query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers serde/transit-write-handlers}])

                           (assoc-in [:formats "application/json" :encoder]
                                     [->json-resultset-encoder {}])

                           (assoc-in [:formats "application/jsonl" :encoder]
                                     [->jsonl-resultset-encoder {}])

                           (assoc-in [:formats "application/json" :encoder]
                                     [->json-resultset-encoder {}])

                           (assoc-in [:formats "application/json" :decoder] (json-query-decoder))))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query] :as query-opts} :body} parameters]
                       {:status 200
                        :body (cond
                                (string? query) (xtp/open-sql-query node query
                                                                    (into {:key-fn #xt/key-fn :snake-case-string}
                                                                          (dissoc query-opts :query)))

                                (seq? query) (xtp/open-xtql-query node query
                                                                  (into {:key-fn #xt/key-fn :snake-case-string}
                                                                        (dissoc query-opts :query)))

                                :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))}))

          :parameters {:body ::query-body}}})

(defmethod route-handler :openapi [_]
  {:get {:handler (fn [_req]
                    (-> (ring-response/resource-response "openapi.yaml")
                        (assoc "Access-Control-Allow-Origin" "*")))
         :muuntaja (m/create m/default-options)}})

(defn- handle-ex-info [ex _req]
  {:status (::status (ex-data ex) 400), :body ex})

(defn- handle-request-coercion-error [ex _req]
  {:status 400
   :body (err/illegal-arg :malformed-request
                          (merge (r.coercion/encode-error (ex-data ex))
                                 {::err/message "malformed request"}))})

(defn- unroll-anomaly [ex]
  (if (instance? Anomaly ex)
    ex
    (when-let [ex (ex-cause ex)]
      (recur ex))))

(defn- handle-muuntaja-decode-error [ex _req]
  (if-let [xt-iae (unroll-anomaly ex)]
    {:status 400
     :body xt-iae}
    {:status 400
     :body (err/illegal-arg :malformed-request
                            (merge {::err/message (str "Malformed " (-> ex ex-data :format pr-str) " request.")}
                                   #_(ex-data ex)))}))

(defn- default-handler [e _]
  {:status 500, :body (throwable->ex-info e)})

(def ^Base64$Decoder base-64-decoder (Base64/getDecoder))

(defn- decode-basic-auth [auth-header]
  (when-let [[_ ^String b64] (re-matches #"Basic (.*)" auth-header)]
    (-> (String. (.decode base-64-decoder b64) StandardCharsets/UTF_8)
        (str/split #":"))))

(def authenticate-interceptor
  {:name ::authenticate
   :enter (fn [{:keys [request] :as ctx}]
            (let [{:keys [node remote-addr]} request
                  authn (authn/<-node node)
                  [user password] (some-> (get-in request [:headers "authorization"])
                                          (decode-basic-auth))
                  success-ctx (-> ctx
                                  ;; to not leak a password in some logs somewhere
                                  (update-in [:request :headers] dissoc "authorization"))]

              (condp = (.methodFor authn user remote-addr)
                #xt.authn/method :trust
                success-ctx

                #xt.authn/method :password
                (if (.verifyPassword authn user password)
                  success-ctx
                  (assoc ctx :error (ex-info (str "password authentication failed for user: " user)
                                             {:type :unauthenticated
                                              ::status 401})))

                (assoc ctx :error (ex-info "authn failed" {:type :unauthenticated
                                                           ::status 401})))))})

(def router
  (let [m (m/create muuntaja-opts)]
    (http/router xtp/http-routes
                 {:expand (fn [{route-name :name, :as route} opts]
                            (r/expand (cond-> route
                                        route-name (merge (route-handler route)))
                                      opts))

                  :data {:muuntaja m
                         :coercion rc.spec/coercion
                         :interceptors [r.swagger/swagger-feature
                                        [ri.parameters/parameters-interceptor]
                                        [ri.muuntaja/format-negotiate-interceptor]

                                        [ri.exception/exception-interceptor
                                         (merge ri.exception/default-handlers
                                                {::ri.exception/default default-handler
                                                 Anomaly handle-ex-info
                                                 :unauthenticated handle-ex-info
                                                 :muuntaja/decode handle-muuntaja-decode-error
                                                 ::r.coercion/request-coercion handle-request-coercion-error
                                                 ::ri.exception/wrap (fn [handler e req]
                                                                       (log/debug e (format "response error (%s): '%s'" (class e) (ex-message e)))
                                                                       (let [response-format (:raw-format (:muuntaja/response req))]
                                                                         (m/format-response m req
                                                                                            (cond-> (handler e req)
                                                                                              (#{"application/jsonl"} response-format)
                                                                                              (assoc :muuntaja/content-type "application/json")))))})]

                                        authenticate-interceptor

                                        [ri.muuntaja/format-response-interceptor]
                                        [ri.muuntaja/format-request-interceptor]
                                        [rh.coercion/coerce-request-interceptor]]}})))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request into opts))})

(defn handler [node]
  (http/ring-handler router
                     (r.ring/create-default-handler)
                     {:executor r.sieppari/executor
                      :interceptors [[with-opts {:node node}]]}))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-server [node ^HttpServer$Factory module]
  (let [port (.getPort module)
        ^Server server (j/run-jetty (handler node)
                                    (merge {:host (some-> (.getHost module) (.getHostAddress))
                                            :port port,
                                            :h2c? true, :h2? true}
                                           {:async? true, :join? false}))]
    (log/info "HTTP server started at" (str (.getURI server)))
    (reify
      HttpServer
      (-http-port [_] (.getPort (.getURI server)))
      XtdbModule
      (close [_]
        (.stop server)
        (log/info "HTTP server stopped.")))))

(defmethod xtn/apply-config! :xtdb/server [^Xtdb$Config config, _ {:keys [host port]}]
  (.module config (cond-> (HttpServer$Factory.)
                    (some? host) (.host (when (not= host "*")
                                          (InetAddress/getByName host)))
                    (some? port) (.port port))))

(defn http-port [^Xtdb node]
  (-http-port (.module node (:on-interface HttpServer))))
