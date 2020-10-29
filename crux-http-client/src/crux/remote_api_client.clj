(ns ^:no-doc crux.remote-api-client
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [crux.codec :as c]
            [crux.error :as err]
            [crux.io :as cio]
            [crux.query-state :as qs]
            [crux.tx :as tx])
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
                                   (edn/read {:readers {'crux/id c/id-edn-reader}
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
                  :node-out-of-sync (err/node-out-of-sync error-data))))

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

(defn- temporal-qps [{:keys [valid-time transact-time tx-id] :as db}]
  {:valid-time (some-> valid-time (cio/format-rfc3339-date))
   :transact-time (some-> transact-time (cio/format-rfc3339-date))
   :tx-id tx-id})

(defrecord RemoteDatasource [url valid-time transact-time tx-id ->jwt-token]
  Closeable
  (close [_])

  ICruxDatasource
  (entity [this eid]
    (api-request-sync (str url "/_crux/entity")
                      {:->jwt-token ->jwt-token
                       :http-opts {:method :get
                                   :query-params (merge (temporal-qps this)
                                                        {:eid-edn (pr-str eid)})}}))

  (entityTx [this eid]
    (api-request-sync (str url "/_crux/entity-tx")
                      {:http-opts {:method :get
                                   :query-params (merge (temporal-qps this)
                                                        {:eid-edn (pr-str eid)})}
                       :->jwt-token ->jwt-token}))

  (query [this q args]
    (with-open [res (.openQuery this q args)]
      (if (:order-by q)
        (vec (iterator-seq res))
        (set (iterator-seq res)))))

  (openQuery [this q args]
    (let [in (api-request-sync (str url "/_crux/query")
                               {:->jwt-token ->jwt-token
                                :http-opts {:as :stream
                                            :method :post
                                            :query-params (temporal-qps this)}
                                :body {:query (pr-str q)
                                       :args (vec args)}})]
      (cio/->cursor #(.close ^Closeable in) (edn-list->lazy-seq in))))

  (entityHistory [this eid opts]
    (with-open [history (.openEntityHistory this eid opts)]
      (vec (iterator-seq history))))

  (openEntityHistory [this eid opts]
    (let [qps (merge (temporal-qps this)
                     {:eid-edn (pr-str eid)
                      :history true

                      :sort-order (name (:sort-order opts))
                      :with-corrections (:with-corrections? opts)
                      :with-docs (:with-docs? opts)

                      :start-valid-time (some-> (:start-valid-time opts) (cio/format-rfc3339-date))
                      :start-transaction-time (some-> (get-in opts [:start-tx ::tx/tx-time])
                                                      (cio/format-rfc3339-date))
                      :start-transaction-id (get-in opts [:start-tx ::tx/tx-id])

                      :end-valid-time (some-> (:end-valid-time opts) (cio/format-rfc3339-date))
                      :end-transaction-time (some-> (get-in opts [:end-tx ::tx/tx-time])
                                                    (cio/format-rfc3339-date))
                      :end-transaction-id (get-in opts [:end-tx ::tx/tx-id])})]
      (if-let [in (api-request-sync (str url "/_crux/entity")
                                    {:http-opts {:as :stream
                                                 :method :get
                                                 :query-params qps}
                                     :->jwt-token ->jwt-token})]
        (cio/->cursor #(.close ^java.io.Closeable in) (edn-list->lazy-seq in))
        cio/empty-cursor)))

  (validTime [_] valid-time)
  (transactionTime [_] transact-time)
  (transaction [_] {:crux.tx/tx-time transact-time, :crux.tx/tx-id tx-id}))

(defrecord RemoteApiClient [url ->jwt-token]
  ICruxAPI
  (db [this] (.db this nil))
  (db [this valid-time]
    (let [^Map tx nil]
      (.db this valid-time tx)))

  (^ICruxDatasource db [this ^Date valid-time ^Date tx-time]
   (let [^Map tx {:crux.tx/tx-time tx-time}]
     (.db this valid-time tx)))

  (^ICruxDatasource db [this ^Date valid-time ^Map tx]
   (let [qps (temporal-qps {:valid-time valid-time
                            :transact-time (:crux.tx/tx-time tx)
                            :tx-id (:crux.tx/tx-id tx)})
         resolved-tx (api-request-sync (str url "/_crux/db")
                                       {:http-opts {:method :get
                                                    :query-params qps}
                                        :->jwt-token ->jwt-token})]
     (->RemoteDatasource url
                         (:crux.db/valid-time resolved-tx)
                         (:crux.tx/tx-time resolved-tx)
                         (:crux.tx/tx-id resolved-tx)
                         ->jwt-token)))

  (openDB [this] (.db this))
  (openDB [this valid-time] (.db this valid-time))
  (^crux.api.ICruxDatasource openDB [this ^Date valid-time ^Date tx-time] (.db this valid-time tx-time))
  (^crux.api.ICruxDatasource openDB [this ^Date valid-time ^Map tx] (.db this valid-time tx))

  (status [_]
    (api-request-sync (str url "/_crux/status")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (attributeStats [_]
    (api-request-sync (str url "/_crux/attribute-stats")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (submitTx [_ tx-ops]
    (try
      (api-request-sync (str url "/_crux/submit-tx")
                        {:body {:tx-ops tx-ops}
                         :->jwt-token ->jwt-token})
      (catch Exception e
        (let [data (ex-data e)]
          (when (and (= 403 (:status data))
                     (string/includes? (:body data) "read-only HTTP node"))
            (throw (UnsupportedOperationException. "read-only HTTP node")))
          (throw e)))))

  (hasTxCommitted [_ submitted-tx]
    (-> (api-request-sync (str url "/_crux/tx-committed")
                          {:http-opts {:method :get
                                       :query-params {:tx-id (:crux.tx/tx-id submitted-tx)}}
                           :->jwt-token ->jwt-token})
        (get :tx-committed?)))

  (openTxLog [this after-tx-id with-ops?]
    (let [in (api-request-sync (str url "/_crux/tx-log")
                               {:http-opts {:method :get
                                            :as :stream
                                            :query-params {:after-tx-id after-tx-id
                                                           :with-ops? with-ops?}}
                                :->jwt-token ->jwt-token})]

      (cio/->cursor #(.close ^Closeable in)
                    (edn-list->lazy-seq in))))

  (sync [_ timeout]
    (-> (api-request-sync (str url "/_crux/sync")
                          {:http-opts {:method :get
                                       :query-params {:timeout (some-> timeout (cio/format-duration-millis))}}
                           :->jwt-token ->jwt-token})
        (get :crux.tx/tx-time)))

  (awaitTxTime [_ tx-time timeout]
    (-> (api-request-sync (str url "/_crux/await-tx-time" )
                          {:http-opts {:method :get
                                       :query-params {:tx-time (cio/format-rfc3339-date tx-time)
                                                      :timeout (some-> timeout (cio/format-duration-millis))}}
                           :->jwt-token ->jwt-token})
        (get :crux.tx/tx-time)))

  (awaitTx [_ tx timeout]
    (api-request-sync (str url "/_crux/await-tx")
                      {:http-opts {:method :get
                                   :query-params {:tx-id (:crux.tx/tx-id tx)
                                                  :timeout (some-> timeout (cio/format-duration-millis))}}
                       :->jwt-token ->jwt-token}))

  (listen [_ opts f]
    (throw (UnsupportedOperationException. "crux/listen not supported on remote clients")))

  (latestCompletedTx [_]
    (api-request-sync (str url "/_crux/latest-completed-tx")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (latestSubmittedTx [_]
    (api-request-sync (str url "/_crux/latest-submitted-tx")
                      {:http-opts {:method :get}
                       :->jwt-token ->jwt-token}))

  (activeQueries [_]
    (->> (api-request-sync (str url "/_crux/active-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  (recentQueries [_]
    (->> (api-request-sync (str url "/_crux/recent-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  (slowestQueries [_]
    (->> (api-request-sync (str url "/_crux/slowest-queries")
                           {:http-opts {:method :get}
                            :->jwt-token ->jwt-token})
         (map qs/->QueryState)))

  Closeable
  (close [_]))

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

(defn new-api-client ^ICruxAPI
  ([url]
   (new-api-client url nil))
  ([url ^RemoteClientOptions options]
   (init-internal-http-request-fn)
   (->RemoteApiClient url (some-> options (.-jwtSupplier) ->jwt-token-fn))))
