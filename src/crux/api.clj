(ns crux.api
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [crux.bootstrap :as b]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.http-server :as srv]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.query :as q]
            [crux.tx :as tx])
  (:import [java.io Closeable InputStreamReader IOException PushbackReader]
           crux.query.QueryDatasource))

(defprotocol CruxDatasource
  (entity [this eid]
    "Returns the document map for an entity.")
  (entity-tx [this eid]
    "Returns the entity tx for an entity.")
  (^java.io.Closeable new-snapshot [this]
   "Returns a new snapshot for usage with q, allowing for lazy results
   in a with-open block.")
  (q [this q] [this snapshot q]
    "Queries the db."))

(extend-protocol CruxDatasource
  QueryDatasource
  (entity [this eid]
    (q/entity this eid))

  (entity-tx [this eid]
    (q/entity-tx this eid))

  (new-snapshot [this]
    (ks/new-snapshot (:kv this)))

  (q [this q]
    (q/q this q))

  (q [this snapshot q]
    (q/q this snapshot q)))

(defprotocol CruxSystem
  (status [this]
    "Returns the status of this node as a map.")
  (db [this] [this business-time] [this business-time transact-time]
    "Returns a db as of optional business and transaction time. Both
    defaulting to now.")
  (history [this eid]
    "Returns the transaction history of an entity.")
  (document [this content-hash]
    "Reads a document from the document store based on its content
    hash.")
  (submit-tx [this tx-ops]
    "Writes transactions to the log for processing.")
  (submitted-tx-updated-entity? [this submitted-tx eid]
    "Checks if a submitted tx did update an entity."))

(defrecord LocalNode [close-promise options kv-store tx-log]
  CruxSystem
  (status [_]
    (srv/status-map kv-store (:bootstrap-servers options)))

  (db [_]
    (q/db kv-store))

  (db [_ business-time]
    (q/db kv-store business-time))

  (db [_ business-time transact-time]
    (q/db kv-store business-time transact-time))

  (history [_ eid]
    (doc/entity-history kv-store eid))

  (document [_ content-hash]
    (let [kv kv-store
          object-store (doc/->DocObjectStore kv)]
      (with-open [snapshot (ks/new-snapshot kv)]
        (get (db/get-objects object-store snapshot [content-hash]) content-hash))))

  (submit-tx [_ tx-ops]
    (db/submit-tx tx-log tx-ops))

  (submitted-tx-updated-entity? [_ submitted-tx eid]
    (q/submitted-tx-updated-entity? kv-store submitted-tx eid))

  Closeable
  (close [_]
    (deliver close-promise true)))

(defn ^LocalNode start-local-node
  "Starts a Crux query node in local library mode.

  For valid options, see crux.bootstrap/cli-options. Options are
  specified as keywords using their long format name,
  like :bootstrap-servers etc.

  Returns a crux.api.LocalNode component that implements
  java.io.Closeable, which allows the system to be stopped by calling
  close."
  [options]
  (let [system-promise (promise)
        close-promise (promise)
        error-promise (promise)
        options (merge b/default-options options)
        node-thread (doto (Thread. (fn []
                                     (try
                                       (b/start-system
                                        options
                                        (fn with-system-callback [system]
                                          (deliver system-promise system)
                                          @close-promise))
                                       (catch Throwable t
                                         (deliver error-promise t))))
                                   "crux.api.local-node-thread")
                      (.start))]
    (while (and (nil? (deref system-promise 100 nil))
                (.isAlive node-thread)))
    (when (realized? error-promise)
      (throw @error-promise))
    (map->LocalNode (merge {:close-promise close-promise
                            :options options}
                           @system-promise))))

(def ^:private remote-api-readers
  {'crux/id idx/new-id
   'crux/entity-tx idx/map->EntityTx
   'crux/submitted-tx tx/map->SubmittedTx})

(defn- edn-list->lazy-seq [in]
  (let [in (PushbackReader. (InputStreamReader. in))
        open-paren \(]
    (when-not (= (int open-paren) (.read in))
      (throw (RuntimeException. "Expected delimiter: (")))
    (->> (repeatedly #(try
                        (edn/read {:eof ::eof
                                   :readers remote-api-readers} in)
                        (catch RuntimeException e
                          (if (= "Unmatched delimiter: )" (.getMessage e))
                            ::eof
                            (throw e)))))
         (take-while #(not= ::eof %)))))

(def ^:private internal-http-request
  (or (try
        (require 'clj-http.client)
        (let [f (resolve 'clj-http.client/request)]
          (fn [url body opts]
            (f (merge {:url url
                       :method :post
                       :body (some-> body pr-str)
                       :as "UTF-8"}
                      opts))))
        (catch IOException not-found))
      (try
        (require 'org.httpkit.client)
        (let [f (resolve 'org.httpkit.client/request)]
          (fn [url body opts]
            @(f (merge {:url url
                        :method :post
                        :body (some-> body pr-str)
                        :as :text}
                       opts))))
        (catch IOException not-found))
      (fn [_ _ _]
        (throw (IllegalStateException. "No supported HTTP client found.")))))

(defn- api-request-sync
  ([url body]
   (api-request-sync url body {}))
  ([url body opts]
   (let [{:keys [body error status headers]
          :as result}
         (internal-http-request url body opts)]
     (cond
       error
       (throw error)

       (= "application/edn" (:content-type headers))
       (if (string? body)
         (edn/read-string {:readers remote-api-readers} body)
         body)

       :else
       (throw (ex-info (str "HTTP status " status) result))))))

(defrecord RemoteSnapshot [streams-state]
  Closeable
  (close [_]
    (doseq [stream @streams-state]
      (.close ^Closeable stream))))

(defn- register-stream-with-remote-snapshot! [snapshot in]
  (swap! (:streams-state snapshot) conj in))

(defrecord RemoteDatasource [url business-time transact-time]
  CruxDatasource
  (entity [this eid]
    (api-request-sync (str url "/entity") {:eid eid
                                           :business-time business-time
                                           :transact-time transact-time}))

  (entity-tx [this eid]
    (api-request-sync (str url "/entity-tx") {:eid eid
                                              :business-time business-time
                                              :transact-time transact-time}))

  (new-snapshot [this]
    (->RemoteSnapshot (atom [])))

  (q [this q]
    (api-request-sync (str url "/q") (assoc q
                                            :business-time business-time
                                            :transact-time transact-time)))

  (q [this snapshot q]
    (let [in (api-request-sync (str url "/q?lazy=true")
                               (assoc q
                                      :business-time business-time
                                      :transact-time transact-time)
                               {:as :stream})]
      (register-stream-with-remote-snapshot! snapshot in)
      (edn-list->lazy-seq in))))

(defrecord RemoteApiClient [url]
  CruxSystem
  (status [_]
    (api-request-sync url nil {:method :get}))

  (db [_]
    (->RemoteDatasource url nil nil))

  (db [_ business-time]
    (->RemoteDatasource url business-time nil))

  (db [_ business-time transact-time]
    (->RemoteDatasource url business-time transact-time))

  (history [_ eid]
    (api-request-sync (str url "/history") eid))

  (document [_ content-hash]
    (api-request-sync (str url "/document") (str content-hash)))

  (submit-tx [_ tx-ops]
    (api-request-sync (str url "/tx-log") tx-ops))

  (submitted-tx-updated-entity? [this {:keys [transact-time tx-id] :as submitted-tx} eid]
    (= tx-id (:tx-id (entity-tx (db this transact-time transact-time) eid))))

  Closeable
  (close [_]))

(defn ^Closeable new-api-client
  "Creates a new remote API client."
  [url]
  (->RemoteApiClient url))
