(ns juxt.crux-lib.async-http-client
; @author refset
; this ns has been adapted from https://github.com/juxt/crux/blob/master/src/crux/bootstrap/remote_api_client.clj
  #?(:cljs (:require [cljs.reader :as edn]
                     [juxt.crux-lib.functions :as fns]
                     [clojure.string :as s]
                     [promesa.core :as p :refer-macros [alet]]
                     [juxt.crux-lib.http-functions :as hf]
                     [promesa.async-cljs :refer-macros [async]]
                     [goog.string :as gs]))
  ;
  #?(:clj  (:require [clojure.tools.reader.edn :as edn]
                     [juxt.crux-lib.functions :as fns]
                     [clojure.string :as s]
                     [promesa.core :as p :refer-macros [alet]]
                     [clojure.java.io :as io]
                     [promesa.async :refer [async]]
                     [clojure.instant :as instant]))
  #?(:clj (:import
            [java.io Closeable InputStreamReader IOException PushbackReader]
            [java.util Date]
            java.text.SimpleDateFormat
            java.time.Duration
            [crux.api Crux ICruxAPI ICruxDatasource])))


; TODO migrate into crux under /async-http-client with an independent deps.edn
; TODO cljc tests, e.g. see https://github.com/tonsky/datascript/blob/master/test/datascript/test.cljc 
; TODO make :cljs version of edn-list->lazy-seq actually lazy
; TODO understand whether RemoteApiStream is needed for cljs and/or how to translate usage of Closeable
; TODO understand whether fetch response.body ReadableStream is compatible with `:as :stream` and update comment, see https://developer.mozilla.org/en-US/docs/Web/API/Streams_API/Using_readable_streams#Consuming_a_fetch_as_a_stream
; TODO potentially merge with crux core in future, for reuse of dependencies, protocols and utility functions


(def format-rfc3339-date
  #?(:cljs ; from https://github.com/metosin/metosin-common/blob/c84fa160548016b7e7cd96555f3d363ad3c2b754/src/cljc/metosin/dates.cljc#L31
     (fn format-rfc3339-date [d]
       (str (.getUTCFullYear d)
            "-" (gs/padNumber (inc (.getUTCMonth d)) 2)
            "-" (gs/padNumber (.getUTCDate d) 2)
            "T" (gs/padNumber (.getUTCHours d) 2)
            ":" (gs/padNumber (.getUTCMinutes d) 2)
            ":" (gs/padNumber (.getUTCSeconds d) 2)
            "." (gs/padNumber (.getUTCMilliseconds d) 3)
            "Z"))
     :clj ; adapted from https://github.com/juxt/crux/blob/321eae4d12210229f49c6a6964c22ea2ff2bf119/src/crux/io.clj#L56
     (fn format-rfc3339-date [^Date d]
       (when d
         (.format ^SimpleDateFormat (.get ^ThreadLocal @#'instant/thread-local-utc-date-format) d)))))



(def edn-list->lazy-seq
  #?(:cljs (fn edn-list->lazy-seq [in]
             (edn/read-string in))
     :clj (fn edn-list->lazy-seq [in]
            (let [in (PushbackReader. (InputStreamReader. in))
                  open-paren \(]
              (when-not (= (int open-paren) (.read in))
                (throw (RuntimeException. "Expected delimiter: (")))
              (->> (repeatedly #(try
                                  (edn/read {:eof ::eof} in)
                                  (catch RuntimeException e
                                    (if (= "Unmatched delimiter: )" (.getMessage e))
                                      ::eof
                                      (throw e)))))
                   (take-while #(not= ::eof %)))))))


(declare internal-http-request-fn)
(comment "Defined to a function that takes a request map and returns a response
         map. The :body for POSTs will be provided as an EDN string by the
         caller. Should return a promise with result body as a string by default,
         or as a stream when the :as :stream option is set.
         Will be called with :url, :method, :body, :headers and
         optionally :as with the value :stream.
         Expects :body, :status and :headers in the response map. Should not
         throw exceptions based on status codes of completed requests.
         Defaults to using js/fetch and clj-http or http-kit if available.")


(defn- init-internal-http-request-fn [] ; todo what's the reason to keep it here?
  (when-not (fn? internal-http-request-fn)
    (def
     internal-http-request-fn
     #?(:cljs #'hf/fetch
        :clj
        (constantly
          (binding [*warn-on-reflection* false]
            (or (try
                  (require 'clj-http.client)
                  (let [f (resolve 'clj-http.client/request)]
                    (p/promise
                      (fn [opts]
                        (f (merge {:as "UTF-8" :throw-exceptions false} opts)))))
                  (catch IOException not-found))
                (try
                  (require 'org.httpkit.client)
                  (let [f (resolve 'org.httpkit.client/request)]
                    (p/promise
                      (fn [opts]
                        (let [{:keys [error] :as result} @(f (merge {:as :text} opts))]
                          (if error
                            (throw error)
                            result)))))
                  (catch IOException not-found))
                (fn [_]
                  (throw (IllegalStateException. "No supported HTTP client found."))))))))))

(defn- api-request-async
  ([url body]
   (api-request-async url body {}))
  ([url body opts]
   (p/alet [result (p/await
                     (internal-http-request-fn
                       (merge {:url url
                               :method :post
                               :headers (cond-> {}
                                                body (assoc "Content-Type" "application/edn"))
                               :body (if (string? body)
                                       body
                                       (some-> body pr-str))}
                              opts)))
            {:keys [body status headers]} result]
     (cond
       (= 404 status)
       nil

       (and (<= 200 status) (< status 400)
            (= "application/edn" (:content-type headers)))
       (if (string? body)
         (edn/read-string body)
         body)

       :else
       (throw (ex-info (str "HTTP status " status) result))))))

(comment
  (defrecord RemoteApiStream [streams-state]
    Closeable
    (close [_]
      (doseq [stream @streams-state]
        (.close ^Closeable stream)))))

(defn- register-stream-with-remote-stream! [snapshot in]
  (swap! (:streams-state snapshot) conj in))

(defn- as-of-map [{:keys [valid-time transact-time] :as datasource}]
  (cond-> {}
    valid-time (assoc :valid-time valid-time)
    transact-time (assoc :transact-time transact-time)))

#?(:cljs
   (defprotocol ICruxDatasource
     (entity [this eid])
     (entityTx [this eid])
     (q [this q] [this snapshot q])
     (historyAscending [this snapshot eid])
     (historyDescending [this snapshot eid])
     (validTime [_])
     (transactionTime [_])))

(defrecord RemoteDatasource [url valid-time transact-time]
  ICruxDatasource
  (entity [this eid]
    (api-request-async (str url "/entity")
                      (assoc (as-of-map this) :eid eid)))

  (entityTx [this eid]
    (api-request-async (str url "/entity-tx")
                      (assoc (as-of-map this) :eid eid)))

 ; (newSnapshot [this]
 ;   (->RemoteApiStream (atom [])))

  (q [this q]
    (api-request-async (str url "/query")
                      (assoc (as-of-map this)
                             :query (fns/normalize-query q))))

  (q [this snapshot q]
    (let [in (api-request-async (str url "/query-stream")
                               (assoc (as-of-map this)
                                      :query (fns/normalize-query q))
                               {:as :stream})]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (historyAscending [this snapshot eid]
    (let [in (api-request-async (str url "/history-ascending")
                               (assoc (as-of-map this) :eid eid)
                               {:as :stream})]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (historyDescending [this snapshot eid]
    (p/alet [in (p/await (api-request-async (str url "/history-descending")
                               (assoc (as-of-map this) :eid eid)
                               {:as :stream}))]
      (register-stream-with-remote-stream! snapshot in)
      (edn-list->lazy-seq in)))

  (validTime [_]
    valid-time)

  (transactionTime [_]
    transact-time))

#?(:cljs
(defprotocol ICruxAPI
  (db [_] [_ valid-time] [_ valid-time transact-time])
  (document [_ content-hash])
  (documents [_ content-hash-set])
  (history [_ eid])
  (historyRange [_ eid valid-time-start transaction-time-start valid-time-end transaction-time-end])
  (status [_])
  (attributeStats [_])
  (submitTx [_ tx-ops])
  (hasSubmittedTxUpdatedEntity [this submitted-tx eid])
  (hasSubmittedTxCorrectedEntity [this submitted-tx valid-time eid])
  (newTxLogContext [_])
  (txLog [_ tx-log-context from-tx-id with-documents?])
  (sync [_ timeout])
  ))

(defn params->query-string [m]
  (clojure.string/join "&" (for [[k v] m] (str (name k) "=" v))))

(defrecord RemoteApiClient [url]
  ICruxAPI
  (db [_]
    (->RemoteDatasource url nil nil))

  (db [_ valid-time]
    (->RemoteDatasource url valid-time nil))

  (db [_ valid-time transact-time]
    (->RemoteDatasource url valid-time transact-time))

  (document [_ content-hash]
    (api-request-async (str url "/document/" content-hash) nil {:method :get}))

  (documents [_ content-hash-set]
    (api-request-async (str url "/documents") content-hash-set {:method :post}))

  (history [_ eid]
    (api-request-async (str url "/history/" eid) nil {:method :get}))

  (historyRange [_ eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (api-request-async (str url "/history-range/" eid "?"
                           (params->query-string
                             (filter second
                               {:valid-time-start       (some-> valid-time-start format-rfc3339-date)
                                :transaction-time-start (some-> transaction-time-start format-rfc3339-date)
                                :valid-time-end         (some-> valid-time-end format-rfc3339-date)
                                :transaction-time-end   (some-> transaction-time-end format-rfc3339-date)})))
                      nil {:method :get}))

  (status [_]
    (api-request-async url nil {:method :get}))

  (attributeStats [_]
    (api-request-async (str url "/attribute-stats") nil {:method :get}))

  (submitTx [_ tx-ops]
    (api-request-async (str url "/tx-log") tx-ops))

  (hasSubmittedTxUpdatedEntity [this {:crux.tx/keys [tx-time tx-id] :as submitted-tx} eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx tx-time eid))

  (hasSubmittedTxCorrectedEntity [this {:crux.tx/keys [tx-time tx-id] :as submitted-tx} valid-time eid]
    (= tx-id (:crux.tx/tx-id (.entityTx (.db this valid-time tx-time) eid))))

 ; (newTxLogContext [_]
 ;   (->RemoteApiStream (atom [])))

  (txLog [_ tx-log-context from-tx-id with-documents?]
    (p/alet [params {:from-tx-id from-tx-id, :with-documents with-documents?}
             qs (params->query-string (filter second params))
             in (p/await (api-request-async
                           (cond-> (str url "/tx-log")
                                   (seq qs) (str "?" qs))
                           nil
                           {:method :get
                            :as :stream}))]
      (register-stream-with-remote-stream! tx-log-context in)
      (edn-list->lazy-seq in)))

  (sync [_ timeout]
    (api-request-async (cond-> (str url "/sync")
                        timeout (str "?timeout=" (.toMillis timeout))) nil {:method :get}))

;  Closeable
;  (close [_])
)

(defn new-api-client ^ICruxAPI
  [url]
  (init-internal-http-request-fn)
  (->RemoteApiClient url))

