(ns crux.api
  "Public API for Crux."
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.tx :as tx])
  (:import [java.io Closeable InputStreamReader IOException PushbackReader]
           [crux.api Crux ICruxSystem ICruxDatasource]))

;; Local Node

(defrecord LocalNode [close-promise kv-store tx-log indexer object-store consumer-config options ^Thread node-thread]
  ICruxSystem
  (db [_]
    (let [tx-time (tx/latest-completed-tx-time indexer)]
      (q/db kv-store tx-time tx-time options)))

  (db [_ business-time]
    (let [tx-time (tx/latest-completed-tx-time indexer)]
      (q/db kv-store business-time tx-time options)))

  (db [_ business-time transact-time]
    (tx/await-tx-time indexer transact-time options)
    (q/db kv-store business-time transact-time options))

  (document [_ content-hash]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/get-single-object object-store snapshot (c/new-id content-hash))))

  (history [_ eid]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (mapv c/entity-tx->edn (idx/entity-history snapshot eid))))

  (status [this]
    (require 'crux.status)
    ((resolve 'crux.status/status-map) this options))

  (submitTx [_ tx-ops]
    @(db/submit-tx tx-log tx-ops))

  (hasSubmittedTxUpdatedEntity [_ submitted-tx eid]
    (tx/await-tx-time indexer (:crux.tx/tx-time submitted-tx) (:crux.tx-log/await-tx-timeout options))
    (q/submitted-tx-updated-entity? kv-store submitted-tx eid))

  (newTxLogContext [_]
    (db/new-tx-log-context tx-log))

  (txLog [_ tx-log-context]
    (db/tx-log tx-log tx-log-context))

  (sync [_ timeout]
    (tx/await-no-consumer-lag indexer (or timeout (:crux.tx-log/await-tx-timeout options))))

  Closeable
  (close [_]
    (some-> close-promise (deliver true))
    (some-> node-thread (.join))))

(defn start-local-node
  "Starts an ICruxSystem query node in local library mode.

  For valid options, see crux.bootstrap/cli-options. Options are
  specified as keywords using their long format name,
  like :bootstrap-servers etc.

  Returns a crux.api.LocalNode component that implements
  java.io.Closeable, which allows the system to be stopped by calling
  close.

  NOTE: requires any KV store dependencies on the classpath. The
  crux.memdb.MemKv KV backend works without additional dependencies.

  The HTTP API can be started by passing the LocalNode to
  crux.http-server/start-http-server. This will require further
  dependencies on the classpath, see crux.http-server for details.

  See also crux.kafka.embedded or crux.api/new-standalone-system for
  self-contained deployments."
  ^ICruxSystem [options]
  (require 'crux.bootstrap)
  (let [system-promise (promise)
        close-promise (promise)
        error-promise (promise)
        options (merge @(resolve 'crux.bootstrap/default-options)
                       options)
        node-thread (doto (Thread. (fn []
                                     (try
                                       ((resolve 'crux.bootstrap/start-system)
                                        options
                                        (fn with-system-callback [system]
                                          (deliver system-promise system)
                                          @close-promise))
                                       (catch Throwable t
                                         (if (realized? system-promise)
                                           (throw t)
                                           (deliver error-promise t)))))
                                   "crux.api.local-node-thread")
                      (.start))]
    (while (and (nil? (deref system-promise 100 nil))
                (.isAlive node-thread)))
    (when (realized? error-promise)
      (throw @error-promise))
    (map->LocalNode (merge {:close-promise close-promise
                            :options options
                            :node-thread node-thread}
                           @system-promise))))

;; Standalone System


(defrecord StandaloneSystem [kv-store tx-log options]
  ICruxSystem
  (db [this]
    (.db ^LocalNode (map->LocalNode this)))

  (db [this business-time]
    (.db ^LocalNode (map->LocalNode this) business-time))

  (db [this business-time transact-time]
    (.db ^LocalNode (map->LocalNode this) business-time transact-time))

  (document [this content-hash]
    (.document ^LocalNode (map->LocalNode this) content-hash))

  (history [this eid]
    (.history ^LocalNode (map->LocalNode this) eid))

  (status [this]
    (.status ^LocalNode (map->LocalNode this)))

  (submitTx [this tx-ops]
    (.submitTx ^LocalNode (map->LocalNode this) tx-ops))

  (hasSubmittedTxUpdatedEntity [this submitted-tx eid]
    (.hasSubmittedTxUpdatedEntity ^LocalNode (map->LocalNode this) submitted-tx eid))

  (newTxLogContext [this]
    (.newTxLogContext ^LocalNode (map->LocalNode this)))

  (txLog [this tx-log-context]
    (.txLog ^LocalNode (map->LocalNode this) tx-log-context))

  (sync [this timeout]
    (.sync ^LocalNode (map->LocalNode this) timeout))

  Closeable
  (close [_]
    (.close ^Closeable kv-store)))

(defn new-standalone-system
  "Creates a minimal standalone system writing the transaction log into
  its local KV store without relying on Kafka.

  Returns a crux.api.StandaloneSystem component that implements
  java.io.Closeable, which allows the system to be stopped by calling
  close.

  NOTE: requires any KV store dependencies on the classpath. The
  crux.memdb.MemKv KV backend works without additional dependencies."
  ^ICruxSystem [{:keys [db-dir kv-backend] :as options}]
  (require 'crux.bootstrap)
  (let [kv-store ((resolve 'crux.bootstrap/start-kv-store) options)
        tx-log (tx/->KvTxLog kv-store)
        object-store (lru/new-cached-object-store kv-store)
        indexer (tx/->KvIndexer kv-store tx-log object-store)]
    (map->StandaloneSystem {:kv-store kv-store
                            :tx-log tx-log
                            :object-store object-store
                            :indexer indexer
                            :options options})))

;; Remote API

(defn- edn-list->lazy-seq [in]
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
         (take-while #(not= ::eof %)))))

(def ^{:doc "Can be rebound using binding or alter-var-root to a
  function that takes a request map and returns a response
  map. The :body for POSTs will be provided as an EDN string by the
  caller. Should return the result body as a string by default, or as
  a stream when the :as :stream option is set.

  Will be called with :url, :method, :body, :headers and
  optionally :as with the value :stream.

  Expects :body, :status, :error and :headers in the response map.

  Defaults to using clj-http or http-kit if available."
       :dynamic true}
  *internal-http-request-fn*
  (or (try
        (require 'clj-http.client)
        (let [f (resolve 'clj-http.client/request)]
          (fn [opts]
            (f (merge {:as "UTF-8"} opts))))
        (catch IOException not-found))
      (try
        (require 'org.httpkit.client)
        (let [f (resolve 'org.httpkit.client/request)]
          (fn [opts]
            @(f (merge {:as :text} opts))))
        (catch IOException not-found))
      (fn [_]
        (throw (IllegalStateException. "No supported HTTP client found.")))))

(defn- api-request-sync
  ([url body]
   (api-request-sync url body {}))
  ([url body opts]
   (let [{:keys [body error status headers]
          :as result}
         (*internal-http-request-fn* (merge {:url url
                                             :method :post
                                             :headers (when body
                                                        {"Content-Type" "application/edn"})
                                             :body (some-> body pr-str)}
                                            opts))]
     (cond
       error
       (throw error)

       (= "application/edn" (:content-type headers))
       (if (string? body)
         (edn/read-string body)
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

(defn- as-of-map [{:keys [business-time transact-time] :as datasource}]
  (cond-> {}
    business-time (assoc :business-time business-time)
    transact-time (assoc :transact-time transact-time)))

(defrecord RemoteDatasource [url business-time transact-time]
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
      (edn-list->lazy-seq in))))

(defrecord RemoteApiClient [url]
  ICruxSystem
  (db [_]
    (->RemoteDatasource url nil nil))

  (db [_ business-time]
    (->RemoteDatasource url business-time nil))

  (db [_ business-time transact-time]
    (->RemoteDatasource url business-time transact-time))

  (document [_ content-hash]
    (api-request-sync (str url "/document/" content-hash) nil {:method :get}))

  (history [_ eid]
    (api-request-sync (str url "/history/" eid) nil {:method :get}))

  (status [_]
    (api-request-sync url nil {:method :get}))

  (submitTx [_ tx-ops]
    (api-request-sync (str url "/tx-log") tx-ops))

  (hasSubmittedTxUpdatedEntity [this {:crux.tx/keys [tx-time tx-id] :as submitted-tx} eid]
    (= tx-id (:crux.tx/tx-id (.entityTx (.db this tx-time tx-time) eid))))

  (newTxLogContext [_]
    (->RemoteApiStream (atom [])))

  (txLog [_ tx-log-context]
    (let [in (api-request-sync (str url "/tx-log")
                               nil
                               {:method :get
                                :as :stream})]
      (register-stream-with-remote-stream! tx-log-context in)
      (edn-list->lazy-seq in)))

  (sync [_ timeout]
    (api-request-sync (str url "/sync?timeout=" timeout) nil {:method :get}))

  Closeable
  (close [_]))

(defn new-api-client
  "Creates a new remote API client ICruxSystem. The remote client
  requires business and transaction time to be specified for all calls
  to ICruxSystem#db.

   NOTE: requires either clj-http or http-kit on the classpath, see
  crux.api/*internal-http-request-fn* for more information."
  ^ICruxSystem [url]
  (->RemoteApiClient url))
