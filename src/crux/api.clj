(ns crux.api
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [crux.bootstrap :as bootstrap]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.http-server :as srv]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.query :as q]
            [crux.tx :as tx]
            [org.httpkit.client :as http])
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

(defrecord LocalNode [close-promise underlying options]
  CruxSystem
  (status [_]
    (srv/status-map (:kv-store @underlying) (:bootstrap-servers options)))

  (db [_]
    (q/db (:kv-store @underlying)))

  (db [_ business-time]
    (q/db (:kv-store @underlying) business-time))

  (db [_ business-time transact-time]
    (q/db (:kv-store @underlying) business-time transact-time))

  (history [_ eid]
    (doc/entity-history (:kv-store @underlying) eid))

  (document [_ content-hash]
    (let [kv (:kv-store @underlying)
          object-store (doc/->DocObjectStore kv)]
      (with-open [snapshot (ks/new-snapshot kv)]
        (get (db/get-objects object-store snapshot [content-hash]) content-hash))))

  (submit-tx [_ tx-ops]
    (db/submit-tx (:tx-log @underlying) tx-ops))

  (submitted-tx-updated-entity? [_ submitted-tx eid]
    (q/submitted-tx-updated-entity? (:kv-store @underlying) submitted-tx eid))

  Closeable
  (close [_] (deliver close-promise true)))

(defn ^Closeable start-local-node
  "Starts a Crux query node in local library mode."
  [start-fn options]
  (log/info "running crux in library mode")
  (let [underlying (atom nil)
        close-promise (promise)
        started-promise (promise)
        options (merge bootstrap/default-options options)
        running-future
        (future
          (log/info "crux thread intialized")
          (bootstrap/start-system
           options
           (fn with-system-callback [system]
             (deliver started-promise true)
             (log/info "crux system start completed")
             (reset! underlying system)
             @close-promise
             (log/info "starting teardown of crux system")))
          (log/info "crux system completed teardown"))]
    (while (not (or (deref started-promise 100 false)
                    (deref running-future 100 false))))
    (->LocalNode close-promise underlying options)))

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

(defn- api-request-sync
  ([url body]
   (api-request-sync url body {}))
  ([url body opts]
   (let [{:keys [body error status headers]
          :as result} @(http/request (merge {:url url
                                             :method :post
                                             :body (some-> body pr-str)
                                             :as :text}
                                            opts))]
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
    ;; TODO: Note that http-kit's client always consumes the entire
    ;; result into memory, so this doesn't currently really work as
    ;; intended.
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
