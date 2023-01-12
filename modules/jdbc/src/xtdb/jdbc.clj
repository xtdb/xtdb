(ns xtdb.jdbc
  (:require [clojure.java.data :as jd]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.connection :as jdbcc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.document-store :as ds]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.tx.event :as txe]
            [xtdb.tx.subscribe :as tx-sub]
            [clojure.tools.logging :as log])
  (:import (clojure.lang MapEntry)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.io Closeable)
           (java.sql Timestamp ResultSet)
           (java.time Duration)
           (java.util Date)))

(defprotocol Dialect
  (setup-schema! [dialect pool])
  (db-type [dialect])

  ;; see #1603/#1707
  (ensure-serializable-identity-seq! [dialect tx table-name]))

(defmulti ->date (fn [d dialect] (db-type dialect)) :default ::default)

(defmethod ->date ::default [t _]
  (assert t)
  (Date. (.getTime ^Timestamp t)))

(defmulti <-blob (fn [blob dialect] (db-type dialect)) :default ::default)

(defmethod <-blob ::default [v _] (nippy/thaw v))

(defrecord HikariConnectionPool [^HikariDataSource pool dialect]
  Closeable
  (close [_]
    (xio/try-close pool)))

(defn ->connection-pool {::sys/deps {:dialect nil}
                         ::sys/args {:pool-opts {:doc "Extra camelCase options to be set on HikariConfig"
                                                 :spec (s/map-of ::sys/keyword any?)}
                                     :db-spec {:doc "db-spec to be passed to next.jdbc"
                                               :spec (s/map-of ::sys/keyword any?)
                                               :required? true}}}
  [{:keys [pool-opts dialect db-spec]}]
  (let [jdbc-url (-> (jdbcc/jdbc-url (merge {:dbtype (name (db-type dialect))} db-spec))
                     ;; mssql doesn't like trailing '?'
                     (str/replace #"\?$" ""))
        pool-opts (merge pool-opts {:jdbcUrl jdbc-url})
        pool (HikariDataSource. (jd/to-java HikariConfig pool-opts))]

    (try
      (setup-schema! dialect pool)
      (catch Throwable t
        (xio/try-close pool)
        (throw t)))

    (->HikariConnectionPool pool dialect)))

(defn- row->log-entry [row dialect]
  (xio/conform-tx-log-entry {::xt/tx-id (long (:event_offset row))
                             ::xt/tx-time (-> (:tx_time row) (->date dialect))}
                            (-> (:v row)
                                (<-blob dialect))))

;; TODO to multimethod?
(defn- tx-result->tx-data [tx-result pool dialect]
  (let [tx-result (condp contains? (db-type dialect)
                    #{:h2 :sqlite :mysql}
                    (let [id (first (vals tx-result))]
                      (jdbc/execute-one! pool ["SELECT * FROM tx_events WHERE EVENT_OFFSET = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps}))

                    #{:mssql}
                    (if-let [id (:generated_keys tx-result)]
                      (jdbc/execute-one! pool ["SELECT * FROM tx_events WHERE EVENT_OFFSET = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})
                      tx-result)

                    #{:oracle}
                    (let [id (first (vals tx-result))]
                      (jdbc/execute-one! pool ["SELECT * FROM tx_events WHERE ROWID = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps}))
                    tx-result)]
    (-> (row->log-entry tx-result dialect)
        (dissoc ::txe/tx-events))))

(defmulti doc-exists-sql
  (fn [dialect doc-id]
    (db-type dialect))
  :default ::default)

(defmethod doc-exists-sql ::default [_ doc-id]
  ["SELECT EVENT_OFFSET from tx_events WHERE EVENT_KEY = ? AND COMPACTED = 0 ORDER BY EVENT_OFFSET FOR UPDATE" doc-id])

(defn- insert-event! [pool event-key v topic]
  (let [b (nippy/freeze v)]
    (jdbc/execute-one! pool ["INSERT INTO tx_events (EVENT_KEY, V, TOPIC, COMPACTED) VALUES (?,?,?,0)" event-key b topic]
                       {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})))

(defn- update-doc! [pool k doc]
  (jdbc/execute! pool ["UPDATE tx_events SET V = ? WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze doc) k]))

(defn- evict-doc! [pool k tombstone]
  (jdbc/execute! pool ["UPDATE tx_events SET V = ?, COMPACTED = 1 WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze tombstone) k]))

(defrecord JdbcDocumentStore [pool dialect]
  db/DocumentStore
  (submit-docs [_ id-and-docs]
    (jdbc/with-transaction [tx pool]
      (doseq [[id doc] (->> (for [[id doc] id-and-docs]
                              (MapEntry/create (str id) doc))
                            (sort-by key))]
        (if (c/evicted-doc? doc)
          (do
            (insert-event! tx id doc "docs")
            (evict-doc! tx id doc))
          (if-not (not-empty (jdbc/execute-one! tx (doc-exists-sql dialect id)))
            (insert-event! tx id doc "docs")
            (update-doc! tx id doc))))))

  (fetch-docs [_ ids]
    (xio/with-nippy-thaw-all
      (->> (for [id-batch (partition-all 100 ids)
                 row (jdbc/execute! pool (into [(format "SELECT EVENT_KEY, V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY IN (%s)"
                                                        (->> (repeat (count id-batch) "?") (str/join ", ")))]
                                               (map (comp str c/new-id) id-batch))
                                    {:builder-fn jdbcr/as-unqualified-lower-maps})]
             row)
           (map (juxt (comp c/new-id c/hex->id-buffer :event_key) #(-> (:v %) (<-blob dialect))))
           (into {})))))

(defn ->document-store {::sys/deps {:connection-pool `->connection-pool
                                    :document-cache 'xtdb.cache/->cache}}
  [{{:keys [pool dialect]} :connection-pool, :keys [document-cache] :as opts}]
  (ds/->cached-document-store
   (assoc opts
          :document-cache document-cache
          :document-store (->JdbcDocumentStore pool dialect))))

(defrecord JdbcTxLog [pool dialect fetch-size ^Closeable tx-consumer]
  db/TxLog
  (submit-tx [this tx-events] (db/submit-tx this tx-events {}))

  (submit-tx [_ tx-events opts]
    (jdbc/with-transaction [tx pool]
      (ensure-serializable-identity-seq! dialect tx "tx_events")
      (let [tx-data (-> (insert-event! tx nil {::txe/tx-events tx-events, ::xt/submit-tx-opts opts} "txs")
                        (tx-result->tx-data tx dialect))]
        (delay
          {::xt/tx-id (::xt/tx-id tx-data)
           ::xt/tx-time (or (::xt/tx-time opts)
                            (::xt/tx-time tx-data))}))))

  (open-tx-log [_ after-tx-id _]
    ;; see https://github.com/xtdb/xtdb/pull/1815 for detail on fetch policy
    (let [conn (jdbc/get-connection pool)
          require-rollback (when (= :postgresql (db-type dialect)) (.setAutoCommit conn false) true)
          sql "SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET > ? ORDER BY EVENT_OFFSET"
          stmt (doto (.prepareStatement conn sql ResultSet/TYPE_FORWARD_ONLY ResultSet/CONCUR_READ_ONLY)
                 (.setFetchSize (if (= :mysql (db-type dialect))
                                  Integer/MIN_VALUE
                                  fetch-size))
                 (.setObject 1 (or after-tx-id 0)))
          rs (.executeQuery stmt)]
      (xio/->cursor (fn []
                      (try
                        (when require-rollback (.rollback conn))
                        (finally
                          (run! xio/try-close [rs stmt conn]))))
                    (->> (resultset-seq rs)
                         (map #(row->log-entry % dialect))))))

  (subscribe [this after-tx-id f]
    (tx-sub/handle-polling-subscription this after-tx-id {:poll-sleep-duration (Duration/ofMillis 100)} f))

  (latest-submitted-tx [_]
    (when-let [max-offset (-> (jdbc/execute-one! pool ["SELECT max(EVENT_OFFSET) AS max_offset FROM tx_events WHERE topic = 'txs'"]
                                                 {:builder-fn jdbcr/as-unqualified-lower-maps})
                              :max_offset)]
      {::xt/tx-id (long max-offset)}))

  Closeable
  (close [_]
    (xio/try-close tx-consumer)))

(defn ->tx-log {::sys/args {:fetch-size {:default 1024, :spec ::sys/pos-int}}
                ::sys/deps {:connection-pool `->connection-pool}}
  [{:keys [connection-pool, fetch-size]}]
  (let [{:keys [pool dialect]} connection-pool]
    (map->JdbcTxLog {:pool pool, :dialect dialect, :fetch-size fetch-size})))
