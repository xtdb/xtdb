(ns crux.remote-api-client
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [crux.io :as cio]
            [crux.codec :as c]
            [crux.query :as q])
  (:import [java.io Closeable InputStreamReader IOException PushbackReader]
           java.time.Duration
           java.util.Date
           [crux.api Crux ICruxAPI ICruxDatasource]))

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
                                             :body (some-> body pr-str)}
                                            opts))]
     (cond
       (= 404 status)
       nil

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
  ICruxDatasource
  (entity [this eid]
    (api-request-sync (str url "/entity")
                      (assoc (as-of-map this) :eid eid)))

  (entityTx [this eid]
    (api-request-sync (str url "/entity-tx")
                      (assoc (as-of-map this) :eid eid)))

  (newSnapshot [this]
    (->RemoteApiStream (atom [])))

  (q [this q]
    (api-request-sync (str url "/query")
                      (assoc (as-of-map this)
                             :query (q/normalize-query q))))

  (q [this snapshot q]
    (let [in (api-request-sync (str url "/query-stream")
                               (assoc (as-of-map this)
                                      :query (q/normalize-query q))
                               {:as :stream})]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (historyAscending [this snapshot eid]
    (let [in (api-request-sync (str url "/history-ascending")
                               (assoc (as-of-map this) :eid eid)
                               {:as :stream})]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (historyDescending [this snapshot eid]
    (let [in (api-request-sync (str url "/history-descending")
                               (assoc (as-of-map this) :eid eid)
                               {:as :stream})]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (validTime [_]
    valid-time)

  (transactionTime [_]
    transact-time))

(defrecord RemoteApiClient [url]
  ICruxAPI
  (db [_]
    (->RemoteDatasource url nil nil))

  (db [_ valid-time]
    (->RemoteDatasource url valid-time nil))

  (db [_ valid-time transact-time]
    (->RemoteDatasource url valid-time transact-time))

  (document [_ content-hash]
    (api-request-sync (str url "/document/" content-hash) nil {:method :get}))

  (documents [_ content-hash-set]
    (api-request-sync (str url "/documents") content-hash-set {:method :post}))

  (history [_ eid]
    (api-request-sync (str url "/history/" eid) nil {:method :get}))

  (historyRange [_ eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (api-request-sync (str url "/history-range/" eid "?"
                           (str/join "&"
                                     (map (partial str/join "=")
                                          [["valid-time-start" (cio/format-rfc3339-date valid-time-start)]
                                           ["transaction-time-start" (cio/format-rfc3339-date transaction-time-start)]
                                           ["valid-time-end" (cio/format-rfc3339-date valid-time-end)]
                                           ["transaction-time-end" (cio/format-rfc3339-date transaction-time-end)]])))
                      nil {:method :get}))

  (status [_]
    (api-request-sync url nil {:method :get}))

  (attributeStats [_]
    (api-request-sync (str url "/attribute-stats") nil {:method :get}))

  (submitTx [_ tx-ops]
    (api-request-sync (str url "/tx-log") tx-ops))

  (hasSubmittedTxUpdatedEntity [this {:crux.tx/keys [tx-time tx-id] :as submitted-tx} eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx tx-time eid))

  (hasSubmittedTxCorrectedEntity [this {:crux.tx/keys [tx-time tx-id] :as submitted-tx} valid-time eid]
    (api-request-sync (str url "/sync?transactionTime=" (cio/format-rfc3339-date tx-time)) nil {:method :get})
    (= tx-id (:crux.tx/tx-id (.entityTx (.db this valid-time tx-time) eid))))

  (newTxLogContext [_]
    (->RemoteApiStream (atom [])))

  (txLog [_ tx-log-context from-tx-id with-documents?]
    (let [params (->> [(when from-tx-id
                         (str "from-tx-id=" from-tx-id))
                       (when with-documents?
                         (str "with-documents=" with-documents?))]
                      (remove nil?)
                      (str/join "&"))
          in (api-request-sync (cond-> (str url "/tx-log")
                                 (seq params) (str "?" params))
                               nil
                               {:method :get
                                :as :stream})]
      (register-stream-with-remote-stream! tx-log-context in)
      (edn-list->lazy-seq in)))

  (sync [_ timeout]
    (api-request-sync (cond-> (str url "/sync")
                        timeout (str "?timeout=" (.toMillis timeout))) nil {:method :get}))

  (sync [_ transaction-time timeout]
    (api-request-sync (cond-> (str url "/sync")
                        transaction-time (str "?transactionTime=" (cio/format-rfc3339-date transaction-time))
                        timeout (str "&timeout=" (cio/format-duration-millis timeout))) nil {:method :get}))

  Closeable
  (close [_]))

(defn new-api-client ^ICruxAPI [url]
  (init-internal-http-request-fn)
  (->RemoteApiClient url))
