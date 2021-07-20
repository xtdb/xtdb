(ns crux.jdbc
  (:require [clojure.java.data :as jd]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.document-store :as ds]
            [crux.io :as cio]
            [crux.system :as sys]
            [crux.tx :as tx]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.connection :as jdbcc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [crux.tx.subscribe :as tx-sub])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.io.Closeable
           java.sql.Timestamp
           java.time.Duration
           java.util.Date))

(defprotocol Dialect
  (setup-schema! [_ pool])
  (db-type [_]))

(defmulti ->date (fn [d dialect] (db-type dialect)) :default ::default)

(defmethod ->date ::default [t _]
  (assert t)
  (Date. (.getTime ^Timestamp t)))

(defmulti <-blob (fn [blob dialect] (db-type dialect)) :default ::default)

(defmethod <-blob ::default [v _] (nippy/thaw v))

(defrecord HikariConnectionPool [^HikariDataSource pool dialect]
  Closeable
  (close [_]
    (cio/try-close pool)))

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
        (cio/try-close pool)
        (throw t)))

    (->HikariConnectionPool pool dialect)))

;; TODO to multimethod?
(defn- tx-result->tx-data [tx-result pool dialect]
  (let [tx-result (condp contains? (db-type dialect)
                    #{:sqlite :mysql}
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
    {::tx/tx-id (long (:event_offset tx-result))
     ::tx/tx-time (-> (:tx_time tx-result) (->date dialect))}))

(defn- insert-event! [pool event-key v topic]
  (let [b (nippy/freeze v)]
    (jdbc/execute-one! pool ["INSERT INTO tx_events (EVENT_KEY, V, TOPIC, COMPACTED) VALUES (?,?,?,0)" event-key b topic]
                       {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})))

(defn- doc-exists? [pool k]
  (not-empty (jdbc/execute-one! pool ["SELECT EVENT_OFFSET from tx_events WHERE EVENT_KEY = ? AND COMPACTED = 0" k])))

(defn- update-doc! [pool k doc]
  (jdbc/execute! pool ["UPDATE tx_events SET V = ? WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze doc) k]))

(defn- evict-doc! [pool k tombstone]
  (jdbc/execute! pool ["UPDATE tx_events SET V = ?, COMPACTED = 1 WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze tombstone) k]))

(defrecord JdbcDocumentStore [pool dialect]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (jdbc/with-transaction [tx pool]
      (doseq [[id doc] (->> (for [[id doc] id-and-docs]
                              (MapEntry/create (str id) doc))
                            (sort-by key))]
        (if (c/evicted-doc? doc)
          (do
            (insert-event! tx id doc "docs")
            (evict-doc! tx id doc))
          (if-not (doc-exists? tx id)
            (insert-event! tx id doc "docs")
            (update-doc! tx id doc))))))

  (fetch-docs [this ids]
    (cio/with-nippy-thaw-all
      (->> (for [id-batch (partition-all 100 ids)
                 row (jdbc/execute! pool (into [(format "SELECT EVENT_KEY, V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY IN (%s)"
                                                        (->> (repeat (count id-batch) "?") (str/join ", ")))]
                                               (map (comp str c/new-id) id-batch))
                                    {:builder-fn jdbcr/as-unqualified-lower-maps})]
             row)
           (map (juxt (comp c/new-id c/hex->id-buffer :event_key) #(-> (:v %) (<-blob dialect))))
           (into {})))))

(defn ->document-store {::sys/deps {:connection-pool `->connection-pool
                                    :document-cache 'crux.cache/->cache}}
  [{{:keys [pool dialect]} :connection-pool, :keys [document-cache] :as opts}]
  (ds/->cached-document-store
   (assoc opts
          :document-cache document-cache
          :document-store (->JdbcDocumentStore pool dialect))))

(defrecord JdbcTxLog [pool dialect ^Closeable tx-consumer]
  db/TxLog
  (submit-tx [_ tx-events]
    (let [tx (-> (insert-event! pool nil tx-events "txs")
                 (tx-result->tx-data pool dialect))]
      (delay tx)))

  (open-tx-log [_ after-tx-id]
    (let [conn (jdbc/get-connection pool)
          stmt (jdbc/prepare conn
                             ["SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET > ? ORDER BY EVENT_OFFSET"
                              (or after-tx-id 0)])
          rs (.executeQuery stmt)]
      (cio/->cursor #(run! cio/try-close [rs stmt conn])
                    (->> (resultset-seq rs)
                         (map (fn [y]
                                {:crux.tx/tx-id (long (:event_offset y))
                                 :crux.tx/tx-time (-> (:tx_time y) (->date dialect))
                                 :crux.tx.event/tx-events (-> (:v y) (<-blob dialect))}))))))

  (subscribe [this after-tx-id f]
    (tx-sub/handle-polling-subscription this after-tx-id {:poll-sleep-duration (Duration/ofMillis 100)} f))

  (latest-submitted-tx [_]
    (when-let [max-offset (-> (jdbc/execute-one! pool ["SELECT max(EVENT_OFFSET) AS max_offset FROM tx_events WHERE topic = 'txs'"]
                                                 {:builder-fn jdbcr/as-unqualified-lower-maps})
                              :max_offset)]
      {:crux.tx/tx-id (long max-offset)}))

  Closeable
  (close [_]
    (cio/try-close tx-consumer)))

(defn ->tx-log {::sys/deps {:connection-pool `->connection-pool}}
  [{{:keys [pool dialect]} :connection-pool}]
  (map->JdbcTxLog {:pool pool, :dialect dialect}))
