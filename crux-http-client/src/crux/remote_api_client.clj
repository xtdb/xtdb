(ns ^:no-doc crux.remote-api-client
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [crux.io :as cio]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.query :as q]
            [clojure.string :as string])
  (:import (java.io Closeable InputStreamReader IOException PushbackReader)
           java.time.Duration
           java.util.Date
           (crux.api Crux ICruxAPI ICruxDatasource NodeOutOfSyncException
                     HistoryOptions HistoryOptions$SortOrder)))

(defn- edn-list->lazy-seq [in]
  (let [in (PushbackReader. (InputStreamReader. in))
        open-paren \(]
    (when-not (= (int open-paren) (.read in))
      (throw (RuntimeException. "Expected delimiter: (")))
    (->> (repeatedly #(try
                        (edn/read {:readers {'crux/id c/id-edn-reader}
                                   :eof ::eof} in)
                        (catch RuntimeException e
                          (if (= "Unmatched delimiter: )" (.getMessage e))
                            ::eof
                            (throw e)))))
         (take-while #(not= ::eof %)))))

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
              (catch IOException not-found))
            (try
              (let [f (requiring-resolve 'org.httpkit.client/request)]
                (fn [opts]
                  (let [{:keys [error] :as result} @(f (merge {:as :text} opts))]
                    (if error
                      (throw error)
                      result))))
              (catch IOException not-found))
            (fn [_]
              (throw (IllegalStateException. "No supported HTTP client found.")))))))))

(defn- api-request-sync
  ([url body]
   (api-request-sync url body {}))
  ([url body opts]
   (let [{:keys [body status headers]
          :as result}
         (*internal-http-request-fn* (merge {:url url
                                             :method :post
                                             :headers (when body
                                                        {"Content-Type" "application/edn"})
                                             :body (some-> body cio/pr-edn-str)}
                                            opts))]
     (cond
       (= 404 status)
       nil

       (= 400 status)
       (let [{:keys [^String cause data]} (edn/read-string (cond-> body
                                                             (= :stream (:as opts)) slurp))]
         (throw (IllegalArgumentException. cause (when data
                                                   (ex-info cause data)))))

       (and (<= 200 status) (< status 400)
            (= "application/edn" (:content-type headers)))
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

(defn- register-stream-with-remote-stream! [snapshot in]
  (swap! (:streams-state snapshot) conj in))

(defn- as-of-map [{:keys [valid-time transact-time] :as datasource}]
  (cond-> {}
    valid-time (assoc :valid-time valid-time)
    transact-time (assoc :transact-time transact-time)))

(defrecord RemoteDatasource [url valid-time transact-time]
  Closeable
  (close [_])

  ICruxDatasource
  (entity [this eid]
    (api-request-sync (str url "/entity/" (str (c/new-id eid)))
                      (as-of-map this)
                      {:method :get}))

  (entityTx [this eid]
    (api-request-sync (str url "/entity-tx/" (str (c/new-id eid)))
                      (as-of-map this)
                      {:method :get}))

  (query [this q]
    (with-open [res (.openQuery this q)]
      (if (:order-by q)
        (vec (iterator-seq res))
        (set (iterator-seq res)))))

  (openQuery [this q]
    (let [in (api-request-sync (str url "/query")
                               (assoc (as-of-map this)
                                      :query (q/normalize-query q))
                               {:as :stream})]
      (cio/->cursor #(.close ^Closeable in)
                    (edn-list->lazy-seq in))))

  (entityHistory [this eid opts]
    (with-open [history (.openEntityHistory this eid opts)]
      (vec (iterator-seq history))))

  (openEntityHistory [this eid opts]
    (let [qps (->> {:sort-order (condp = (.sortOrder opts)
                                  HistoryOptions$SortOrder/ASC (name :asc)
                                  HistoryOptions$SortOrder/DESC (name :desc))
                    :with-corrections (.withCorrections opts)
                    :with-docs (.withDocs opts)
                    :start-valid-time (some-> (.startValidTime opts) (cio/format-rfc3339-date))
                    :start-transaction-time (some-> (.startTransactionTime opts) (cio/format-rfc3339-date))
                    :end-valid-time (some-> (.endValidTime opts) (cio/format-rfc3339-date))
                    :end-transaction-time (some-> (.endTransactionTime opts) (cio/format-rfc3339-date))
                    :valid-time (cio/format-rfc3339-date valid-time)
                    :transaction-time (cio/format-rfc3339-date transact-time)}
                   (into {} (remove (comp nil? val))))]
      (if-let [in (api-request-sync (str url "/entity-history/" (c/new-id eid))
                                    nil
                                    {:as :stream,
                                     :method :get
                                     :query-params qps})]
        (cio/->cursor #(.close ^java.io.Closeable in)
                      (edn-list->lazy-seq in))
        (cio/->cursor #() []))))

  (validTime [_] valid-time)
  (transactionTime [_] transact-time))

(defrecord RemoteApiClient [url]
  ICruxAPI
  (db [_] (->RemoteDatasource url nil nil))
  (db [_ valid-time] (->RemoteDatasource url valid-time nil))

  (db [_ valid-time tx-time]
    (when tx-time
      (let [latest-tx-time (-> (api-request-sync (str url "/latest-completed-tx") nil {:method :get})
                               :crux.tx/tx-time)]
        (when (or (nil? latest-tx-time) (pos? (compare tx-time latest-tx-time)))
          (throw (NodeOutOfSyncException.
                  (format "Node hasn't indexed the transaction: requested: %s, available: %s" tx-time latest-tx-time)
                  tx-time latest-tx-time)))))

    (->RemoteDatasource url valid-time tx-time))

  (openDB [this] (.db this))
  (openDB [this valid-time] (.db this valid-time))
  (openDB [this valid-time tx-time] (.db this valid-time tx-time))

  (status [_]
    (api-request-sync url nil {:method :get}))

  (attributeStats [_]
    (api-request-sync (str url "/attribute-stats") nil {:method :get}))

  (submitTx [_ tx-ops]
    (try
      (api-request-sync (str url "/tx-log") tx-ops)
      (catch Exception e
        (let [data (ex-data e)]
          (when (and (= 403 (:status data))
                     (string/includes? (:body data) "read-only HTTP node"))
            (throw (UnsupportedOperationException. "read-only HTTP node")))
          (throw e)))))

  (hasTxCommitted [_ submitted-tx]
    (api-request-sync (str url "/tx-committed?tx-id=" (:crux.tx/tx-id submitted-tx)) nil {:method :get}))

  (openTxLog [this after-tx-id with-ops?]
    (let [params (->> [(when after-tx-id
                         (str "after-tx-id=" after-tx-id))
                       (when with-ops?
                         (str "with-ops=" with-ops?))]
                      (remove nil?)
                      (str/join "&"))
          in (api-request-sync (cond-> (str url "/tx-log")
                                 (seq params) (str "?" params))
                               nil
                               {:method :get
                                :as :stream})]

      (cio/->cursor #(.close ^Closeable in)
                    (edn-list->lazy-seq in))))

  (sync [_ timeout]
    (api-request-sync (cond-> (str url "/sync")
                        timeout (str "?timeout=" (.toMillis timeout))) nil {:method :get}))

  (awaitTxTime [_ tx-time timeout]
    (api-request-sync (cond-> (str url "/await-tx-time?tx-time=" (cio/format-rfc3339-date tx-time))
                        timeout (str "&timeout=" (cio/format-duration-millis timeout))) nil {:method :get}))

  (awaitTx [_ tx timeout]
    (api-request-sync (cond-> (str url "/await-tx?tx-id=" (:crux.tx/tx-id tx))
                        timeout (str "&timeout=" (cio/format-duration-millis timeout))) nil {:method :get}))

  (listen [_ opts f]
    (throw (UnsupportedOperationException. "crux/listen not supported on remote clients")))

  (latestCompletedTx [_]
    (api-request-sync (str url "/latest-completed-tx") nil {:method :get}))

  (latestSubmittedTx [_]
    (api-request-sync (str url "/latest-submitted-tx") nil {:method :get}))

  Closeable
  (close [_]))

(defn new-api-client ^ICruxAPI [url]
  (init-internal-http-request-fn)
  (->RemoteApiClient url))
