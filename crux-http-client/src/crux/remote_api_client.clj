(ns ^:no-doc crux.remote-api-client
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.error :as err]
            [crux.io :as cio]
            [crux.query-state :as qs]
            [crux.tx :as tx]
            [crux.api :as api])
  (:import com.nimbusds.jwt.SignedJWT
           [crux.api HistoryOptions$SortOrder ICruxAPI ICruxDatasource RemoteClientOptions NodeOutOfSyncException]
           [java.io Closeable InputStreamReader IOException PushbackReader]
           java.time.Instant
           (java.util Date Map)
           java.util.function.Supplier))

(defn- edn-list->lazy-seq [in]
  (let [in (PushbackReader. (InputStreamReader. in))]
    (condp = (.read in)
      -1 nil
      (int \() (->> (repeatedly #(try
                                   (edn/read {:readers {'crux/id c/id-edn-reader
                                                        'crux/query-state qs/->QueryState
                                                        'crux/query-error qs/->QueryError}
                                              :eof ::eof} in)
                                   (catch RuntimeException e
                                     (if (= "Unmatched delimiter: )" (.getMessage e))
                                       ::eof
                                       (throw e)))))
                    (take-while #(not= ::eof %)))

      (throw (RuntimeException. "Expected delimiter: (")))))

(def ^{:doc "Can be rebound using binding or alter-var-root to a
  function that takes a request map and returns a response
  map. The :body for POSTs will be provided as an EDN string by the
  caller. Should return the result body as a string by default, or as
  a stream when the :as :stream option is set.

  Will be called with :url, :method, :body, :headers and
  optionally :as with the value :stream.

  Expects :body, :status and :headers in the response map. Should not
  throw exceptions based on status codes of completed requests.

  Defaults to using clj-http or http-kit if available."
       :dynamic true}
  *internal-http-request-fn*)

(defn- init-internal-http-request-fn []
  (when (not (bound? #'*internal-http-request-fn*))
    (alter-var-root
     #'*internal-http-request-fn*
     (constantly
      (binding [*warn-on-reflection* false]
        (or (try
              (let [f (requiring-resolve 'clj-http.client/request)]
                (fn [opts]
                  (f (merge {:as "UTF-8" :throw-exceptions false} opts))))
              (catch IOException _e))
            (try
              (let [f (requiring-resolve 'org.httpkit.client/request)]
                (fn [opts]
                  (let [{:keys [error] :as result} @(f (merge {:as :text} opts))]
                    (if error
                      (throw error)
                      result))))
              (catch IOException _e))
            (fn [_]
              (throw (IllegalStateException. "No supported HTTP client found.")))))))))

(defn- api-request-sync
  ([url {:keys [body http-opts ->jwt-token]}]
   (let [{:keys [body status] :as result}
         (*internal-http-request-fn* (merge {:url url
                                             :method :post
                                             :headers (merge (when body
                                                               {"Content-Type" "application/edn"})
                                                             (when ->jwt-token
                                                               {"Authorization" (str "Bearer " (->jwt-token))}))
                                             :body (some-> body cio/pr-edn-str)
                                             :accept :edn}
                                            (update http-opts :query-params #(into {} (remove (comp nil? val) %)))))]
     (cond
       (= 404 status)
       nil

       (= 400 status)
       (let [error-data (edn/read-string (cond-> body
                                           (= :stream (:as http-opts)) slurp))]
         (throw (case (::err/error-type error-data)
                  :illegal-argument (err/illegal-arg (::err/error-key error-data) error-data)
                  :node-out-of-sync (err/node-out-of-sync error-data)
                  (ex-info "Generic remote client error" error-data))))

       (and (<= 200 status) (< status 400))
       (if (string? body)
         (c/read-edn-string-with-readers body)
         body)

       :else
       (throw (ex-info (str "HTTP status " status) result))))))

(defrecord RemoteApiStream [streams-state]
  Closeable
  (close [_]
    (doseq [stream @streams-state]
      (.close ^Closeable stream))))

(defn- temporal-qps [{:keys [valid-time tx-time tx-id] :as db}]
  {:valid-time (some-> valid-time (cio/format-rfc3339-date))
   :tx-time (some-> tx-time (cio/format-rfc3339-date))
   :tx-id tx-id})

(defrecord RemoteDatasource [url valid-time tx-time tx-id ->jwt-token]
  Closeable
  (close [_])

  api/PCruxDatasource
  (entity [this eid]
    (api-request-sync (str url "/_crux/entity")
                      {:->jwt-token ->jwt-token
                       :http-opts {:method :get
                                   :query-params (merge (temporal-qps this)
                                                        {:eid-edn (pr-str eid)})}}))
  (entity-tx [this eid]
    (api-request-sync (str url "/_crux/entity-tx")
                      {:http-opts {:method :get
                                   :query-params (merge (temporal-qps this)
                                                        {:eid-edn (pr-str eid)})}
                       :->jwt-token ->jwt-token}))

  (q* [this query in-args]
    (with-open [res (api/open-q* this query in-args)]
      (if (:order-by query)
        (vec (iterator-seq res))
        (set (iterator-seq res)))))

  (open-q* [this query in-args]
    (let [in (api-request-sync (str url "/_crux/query")
                               {:->jwt-token ->jwt-token
                                :http-opts {:as :stream
                                            :method :post
                                            :query-params (temporal-qps this)}
                                :body {:query (pr-str query)
                                       :in-args (vec in-args)}})]
      (cio/->cursor #(.close ^Closeable in) (edn-list->lazy-seq in))))

  (pull [this projection eid]
    (let [?eid (gensym '?eid)
          projection (cond-> projection (string? projection) c/read-edn-string-with-readers)]
      (->> (api/q this
                  {:find [(list 'pull ?eid projection)]
                   :in [?eid]}
                  eid)
           ffirst)))

  (pull-many [this projection eids]
    (let [?eid (gensym '?eid)
          projection (cond-> projection (string? projection) c/read-edn-string-with-readers)]
      (->> (api/q this
                  {:find [(list 'pull ?eid projection)]
                   :in [[?eid '...]]}
                  this)
           (mapv first))))

  ;; TODO should we make the Clojure history opts the same format (`:start-valid-time`, `:start-tx`)
  ;; as the new Java ones?
  (entity-history [this eid sort-order] (api/entity-history this eid sort-order {}))

  (entity-history [this eid sort-order opts]
   (with-open [history (api/open-entity-history this eid sort-order opts)]
     (vec (iterator-seq history))))

  (open-entity-history [this eid sort-order] (api/open-entity-history this eid sort-order {}))

  (open-entity-history [this eid sort-order opts]
   (let [opts (assoc opts :sort-order sort-order)
         qps (merge (temporal-qps this)
                    {:eid-edn (pr-str eid)
                     :history true

                     :sort-order (name sort-order)
                     :with-corrections (:with-corrections? opts)
                     :with-docs (:with-docs? opts)

                     :start-valid-time (some-> (:start-valid-time opts) (cio/format-rfc3339-date))
                     :start-tx-time (some-> (get-in opts [:start-tx ::tx/tx-time])
                                            (cio/format-rfc3339-date))
                     :start-tx-id (get-in opts [:start-tx ::tx/tx-id])

                     :end-valid-time (some-> (:end-valid-time opts) (cio/format-rfc3339-date))
                     :end-tx-time (some-> (get-in opts [:end-tx ::tx/tx-time])
                                          (cio/format-rfc3339-date))
                     :end-tx-id (get-in opts [:end-tx ::tx/tx-id])})]
     (if-let [in (api-request-sync (str url "/_crux/entity")
                                   {:http-opts {:as :stream
                                                :method :get
                                                :query-params qps}
                                    :->jwt-token ->jwt-token})]
       (cio/->cursor #(.close ^java.io.Closeable in) (edn-list->lazy-seq in))
       cio/empty-cursor)))

  (valid-time [this] valid-time)

  (transaction-time [this] tx-time)

  (db-basis [this]
    {:crux.db/valid-time valid-time
     :crux.tx/tx {:crux.tx/tx-time tx-time
                  :crux.tx/tx-id tx-id}}))

(defrecord RemoteApiClient [url ->jwt-token]
  Closeable
  (close [_])

  api/DBProvider
  (db [this] (api/db this {}))

  (db [this valid-time tx-time]
   (api/db this {:crux.db/valid-time valid-time
                 :crux.tx/tx-time tx-time}))

  (db [this valid-time-or-basis]
   (if (instance? Date valid-time-or-basis)
     (api/db this {:crux.db/valid-time valid-time-or-basis})
     (let [db-basis valid-time-or-basis
           qps (temporal-qps {:valid-time (:crux.db/valid-time db-basis)
                              :tx-time (or (get-in db-basis [:crux.tx/tx :crux.tx/tx-time])
                                           (:crux.tx/tx-time db-basis))
                              :tx-id (or (get-in db-basis [:crux.tx/tx :crux.tx/tx-id])
                                         (:crux.tx/tx-id db-basis))})
           resolved-tx (api-request-sync (str url "/_crux/db")
                                         {:http-opts {:method :get
                                                      :query-params qps}
                                          :->jwt-token ->jwt-token})]
       (->RemoteDatasource url
                           (:crux.db/valid-time resolved-tx)
                           (get-in resolved-tx [:crux.tx/tx :crux.tx/tx-time])
                           (get-in resolved-tx [:crux.tx/tx :crux.tx/tx-id])
                           ->jwt-token))))

  (open-db [this] (api/db this))

  (open-db [this valid-time-or-basis] (api/db this valid-time-or-basis))

  (open-db [this valid-time tx-time] (api/db this valid-time tx-time))

  api/PCruxNode
  (status [this]
    (api-request-sync (str url "/_crux/status")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (tx-committed? [this submitted-tx]
    (-> (api-request-sync (str url "/_crux/tx-committed")
                          {:http-opts {:method :get
                                       :query-params {:tx-id (:crux.tx/tx-id submitted-tx)}}
                           :->jwt-token ->jwt-token})
        (get :tx-committed?)))

  (sync [this] (api/sync this nil))

  (sync [this timeout]
   (-> (api-request-sync (str url "/_crux/sync")
                         {:http-opts {:method :get
                                      :query-params {:timeout (some-> timeout (cio/format-duration-millis))}}
                          :->jwt-token ->jwt-token})
       (get :crux.tx/tx-time)))

  (sync [this tx-time timeout]
   (defonce warn-on-deprecated-sync
            (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
   (api/await-tx-time this tx-time timeout))

  (await-tx [this submitted-tx] (api/await-tx this submitted-tx nil))
  (await-tx [this submitted-tx timeout]
    (api-request-sync (str url "/_crux/await-tx")
                      {:http-opts {:method :get
                                   :query-params {:tx-id (:crux.tx/tx-id submitted-tx)
                                                 :timeout (some-> timeout (cio/format-duration-millis))}}
                       :->jwt-token ->jwt-token}))

  (await-tx-time [this tx-time] (api/await-tx-time this tx-time nil))
  (await-tx-time [this tx-time timeout]
    (-> (api-request-sync (str url "/_crux/await-tx-time" )
                          {:http-opts {:method :get
                                       :query-params {:tx-time (cio/format-rfc3339-date tx-time)
                                                     :timeout (some-> timeout (cio/format-duration-millis))}}
                           :->jwt-token ->jwt-token})
        (get :crux.tx/tx-time)))

  (listen [this event-opts f]
    (throw (UnsupportedOperationException. "crux/listen not supported on remote clients")))

  (latest-completed-tx [this]
    (api-request-sync (str url "/_crux/latest-completed-tx")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))



  (latest-submitted-tx [this]
    (api-request-sync (str url "/_crux/latest-submitted-tx")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (attribute-stats [this]
    (api-request-sync (str url "/_crux/attribute-stats")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (active-queries [this]
    (->> (api-request-sync (str url "/_crux/active-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  (recent-queries [this]
    (->> (api-request-sync (str url "/_crux/recent-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  (slowest-queries [this]
    (->> (api-request-sync (str url "/_crux/slowest-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  api/PCruxIngestClient
  (submit-tx [this tx-ops]
    (let [tx-ops (api/conform-tx-ops tx-ops)]
      (try
        (api-request-sync (str url "/_crux/submit-tx")
                          {:body {:tx-ops tx-ops}
                           :->jwt-token ->jwt-token})
        (catch Exception e
          (let [data (ex-data e)]
            (when (and (= 403 (:status data))
                       (string/includes? (:body data) "read-only HTTP node"))
              (throw (UnsupportedOperationException. "read-only HTTP node")))
            (throw e))))
      ))

  (open-tx-log ^crux.api.ICursor [this after-tx-id with-ops?]
    (let [with-ops? (boolean with-ops?)
          in (api-request-sync (str url "/_crux/tx-log")
                               {:http-opts {:method :get
                                            :as :stream
                                            :query-params {:after-tx-id after-tx-id
                                                           :with-ops? with-ops?}}
                                :->jwt-token ->jwt-token})]

      (cio/->cursor #(.close ^Closeable in)
                    (edn-list->lazy-seq in)))))

(defn- ^:dynamic *now* ^Instant []
  (Instant/now))

(defn- ->jwt-token-fn [^Supplier jwt-supplier]
  (let [!token-cache (atom nil)]
    (fn []
      (or (when-let [token @!token-cache]
            (let [expiration-time (.minusSeconds ^Instant (:token-expiration token) 5)
                  current-time (*now*)]
              (when (.isBefore current-time expiration-time)
                (:token token))))
          (let [^String new-token (.get jwt-supplier)
                ^Instant new-token-exp (-> (SignedJWT/parse new-token)
                                           (.getJWTClaimsSet)
                                           (.getExpirationTime)
                                           (.toInstant))]
            (reset! !token-cache {:token new-token :token-expiration new-token-exp})
            new-token)))))

(defn new-api-client
  ([url]
   (new-api-client url nil))
  ([url ^RemoteClientOptions options]
   (init-internal-http-request-fn)
   (->RemoteApiClient url (some-> options (.-jwtSupplier) ->jwt-token-fn))))
