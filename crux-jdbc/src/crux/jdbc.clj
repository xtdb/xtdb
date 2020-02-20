(ns crux.jdbc
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.tx.consumer :as tc]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as jdbcc]
            [next.jdbc.result-set :as jdbcr]
            [taoensso.nippy :as nippy]
            [clojure.string :as str]
            [crux.io :as cio])
  (:import com.zaxxer.hikari.HikariDataSource
           crux.tx.consumer.Message
           [java.util.concurrent LinkedBlockingQueue TimeUnit]
           java.util.Date))

(defn- dbtype->crux-jdbc-dialect [dbtype]
  (condp contains? dbtype
    #{"h2"} :h2
    #{"mysql"} :mysql
    #{"sqlite"} :sqlite
    #{"postgresql" "pgsql"} :psql
    #{"oracle"} :oracle))

(defmulti setup-schema! (fn [dbtype ds] (dbtype->crux-jdbc-dialect dbtype)))

(defmulti prep-for-tests! (fn [dbtype ds] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod prep-for-tests! :default [_ ds] (jdbc/execute! ds ["DROP TABLE IF EXISTS tx_events"]))

(defmulti ->date (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->date :default [_ t]
  (assert t)
  (java.util.Date. (.getTime ^java.sql.Timestamp t)))

(defmulti ->v (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->v :default [_ v] (nippy/thaw v))

(defmulti ->pool-options (fn [dbtype options] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->pool-options :default [_ options] options)

(deftype Tx [^Date time ^long id])

(defn- tx-result->tx-data [ds dbtype tx-result]
  (let [tx-result (condp contains? dbtype
                    #{"sqlite" "mysql"}
                    (let [id (first (vals tx-result))]
                      (jdbc/execute-one! ds ["SELECT * FROM tx_events WHERE EVENT_OFFSET = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps}))

                    #{"oracle"}
                    (let [id (first (vals tx-result))]
                      (jdbc/execute-one! ds ["SELECT * FROM tx_events WHERE ROWID = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps}))
                    tx-result)]
    (let [tx-id (:event_offset tx-result)
          tx-time (:tx_time tx-result)]
      (Tx. (->date dbtype tx-time) tx-id))))

(defn- insert-event! [ds event-key v topic]
  (let [b (nippy/freeze v)]
    (jdbc/execute-one! ds ["INSERT INTO tx_events (EVENT_KEY, V, TOPIC, COMPACTED) VALUES (?,?,?,0)" event-key b topic]
                       {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})))

(defn- doc-exists? [ds k]
  (not-empty (jdbc/execute-one! ds ["SELECT EVENT_OFFSET from tx_events WHERE EVENT_KEY = ? AND COMPACTED = 0" k])))

(defn- evict-docs! [ds k tombstone]
  (jdbc/execute! ds ["UPDATE tx_events SET V = ?, COMPACTED = 1 WHERE TOPIC = 'docs' AND EVENT_KEY = ?" tombstone k]))

(defrecord JdbcTxLog [ds dbtype]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (doseq [[id doc] id-and-docs
            :let [id (str id)]]
      (if (idx/evicted-doc? doc)
        (do
          (insert-event! ds id doc "docs")
          (evict-docs! ds id (nippy/freeze doc)))
        (if-not (doc-exists? ds id)
          (insert-event! ds id doc "docs")
          (log/infof "Skipping doc insert %s" id)))))

  db/TxLog
  (submit-tx [this tx-ops]
    (let [tx-events (map tx/tx-op->tx-event tx-ops)
          ^Tx tx (tx-result->tx-data ds dbtype (insert-event! ds nil tx-events "txs"))]
      (delay {:crux.tx/tx-id (.id tx)
              :crux.tx/tx-time (.time tx)})))

  (open-tx-log [this from-tx-id]
    (let [stmt (jdbc/prepare (jdbc/get-connection ds)
                             ["SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET >= ? ORDER BY EVENT_OFFSET"
                              (or from-tx-id 0)])
          rs (.executeQuery stmt)]
      (db/->closeable-tx-log-iterator
       #(run! cio/try-close [rs stmt])
       (->> (resultset-seq rs)
            (map (fn [y]
                   {:crux.tx/tx-id (int (:event_offset y))
                    :crux.tx/tx-time (->date dbtype (:tx_time y))
                    :crux.tx.event/tx-events (->v dbtype (:v y))}))))))

  (latest-submitted-tx [this]
    (when-let [max-offset (-> (jdbc/execute-one! ds ["SELECT max(EVENT_OFFSET) AS max_offset FROM tx_events"]
                                                 {:builder-fn jdbcr/as-unqualified-lower-maps})
                              :max_offset)]
      {:crux.tx/tx-id max-offset})))

(defn- event-result->message [dbtype result]
  (Message. (->v dbtype (:v result))
            nil
            (:event_offset result)
            (->date dbtype (:tx_time result))
            (:event_key result)
            {:crux.tx/sub-topic (keyword (:topic result))}))

(defrecord JDBCQueue [ds dbtype]
  tc/OffsetBasedQueue
  (next-events-from-offset [this offset]
    (mapv (partial event-result->message dbtype)
          (jdbc/execute! ds
                         ["SELECT EVENT_OFFSET, EVENT_KEY, TX_TIME, V, TOPIC FROM tx_events WHERE EVENT_OFFSET >= ? ORDER BY EVENT_OFFSET" offset]
                         {:max-rows 100 :builder-fn jdbcr/as-unqualified-lower-maps}))))

(defn conform-next-jdbc-properties [m]
  (into {} (->> m
                (filter (fn [[k]] (= "crux.jdbc" (namespace k))))
                (map (fn [[k v]] [(keyword (name k)) v])))))

(def ^:private require-lock 'lock)

(defn- start-jdbc-ds [_ options]
  (let [{:keys [dbtype] :as options} (conform-next-jdbc-properties options)]
    (locking require-lock
      (require (symbol (str "crux.jdbc." (name (dbtype->crux-jdbc-dialect dbtype))))))
    (let [ds (jdbcc/->pool HikariDataSource (->pool-options dbtype options))]
      (setup-schema! dbtype ds)
      ds)))

(defn- start-tx-log [{::keys [ds]} {::keys [dbtype]}]
  (map->JdbcTxLog {:ds ds :dbtype dbtype}))

(defn- start-event-log-consumer [{:keys [crux.node/indexer crux.jdbc/ds]} {::keys [dbtype]}]
  (tc/start-indexing-consumer {:queue (tc/offsets-based-queue indexer (JDBCQueue. ds dbtype))
                               :index-fn (partial tc/index-records indexer)
                               :idle-sleep-ms 10}))

(def topology (merge n/base-topology
                     {::ds {:start-fn start-jdbc-ds
                            :args {::dbtype {:doc "Database type"
                                             :required? true
                                             :crux.config/type :crux.config/string}
                                   ::dbname {:doc "Database name"
                                             :required? true
                                             :crux.config/type :crux.config/string}}}
                      ::event-log-consumer {:start-fn start-event-log-consumer
                                            :deps [:crux.node/indexer ::ds]}
                      :crux.node/tx-log {:start-fn start-tx-log
                                         :deps [::ds]}
                      :crux.node/document-store {:start-fn (fn [{:keys [:crux.node/tx-log]} _] tx-log)
                                                        :deps [:crux.node/tx-log]}}))
