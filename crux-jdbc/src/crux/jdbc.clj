(ns crux.jdbc
  (:require [clojure.core.reducers :as r]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            crux.api
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.node :as n]
            [crux.tx :as tx]
            crux.tx.consumer
            [crux.tx.polling :as p]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as jdbcc]
            [next.jdbc.result-set :as jdbcr]
            [taoensso.nippy :as nippy])
  (:import com.zaxxer.hikari.HikariDataSource
           crux.tx.consumer.Message
           java.io.Closeable
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

(defrecord JdbcLogQueryContext [running?]
  Closeable
  (close [_]
    (reset! running? false)))

(defn- doc-exists? [ds k]
  (not-empty (jdbc/execute-one! ds ["SELECT EVENT_OFFSET from tx_events WHERE EVENT_KEY = ? AND COMPACTED = 0" k])))

(defn- evict-docs! [ds k tombstone]
  (jdbc/execute! ds ["UPDATE tx_events SET V = ?, COMPACTED = 1 WHERE TOPIC = 'docs' AND EVENT_KEY = ?" tombstone k]))

(defrecord JdbcTxLog [ds dbtype]
  db/TxLog
  (submit-doc [this content-hash doc]
    (let [id (str content-hash)]
      (if (idx/evicted-doc? doc)
        (do
          (insert-event! ds id doc "docs")
          (evict-docs! ds id (nippy/freeze doc)))
        (if-not (doc-exists? ds id)
          (insert-event! ds id doc "docs")
          (log/infof "Skipping doc insert %s" id)))))

  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (doseq [doc (tx/tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx/tx-ops->tx-events tx-ops)
          ^Tx tx (tx-result->tx-data ds dbtype (insert-event! ds nil tx-events "txs"))]
      (delay {:crux.tx/tx-id (.id tx)
              :crux.tx/tx-time (.time tx)})))

  (new-tx-log-context [this]
    (JdbcLogQueryContext. (atom true)))

  (tx-log [this tx-log-context from-tx-id]
    (let [^LinkedBlockingQueue q (LinkedBlockingQueue. 1000)
          running? (:running? tx-log-context)
          consumer-f (fn [_ y]
                       (.put q {:crux.tx/tx-id (int (:event_offset y))
                                :crux.tx/tx-time (->date dbtype (:tx_time y))
                                :crux.tx.event/tx-events (->v dbtype (:v y))})
                       (if @running?
                         nil
                         (reduced nil)))]
      (future
        (try
          (r/reduce consumer-f
                    nil
                    (jdbc/plan ds ["SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET >= ? ORDER BY EVENT_OFFSET"
                                   (or from-tx-id 0)]
                               {:builder-fn jdbcr/as-unqualified-lower-maps}))
          (catch Throwable t
            (log/error t "Exception occured reading event log")))
        (.put q -1))
      ((fn step []
         (lazy-seq
          (when-let [x (.poll q 5000 TimeUnit/MILLISECONDS)]
            (when (not= -1 x)
              (cons x (step))))))))))

(defn- event-result->message [dbtype result]
  (Message. (->v dbtype (:v result))
            nil
            (:event_offset result)
            (->date dbtype (:tx_time result))
            (:event_key result)
            {:crux.tx/sub-topic (keyword (:topic result))}))

(defrecord JDBCEventLogConsumer [ds dbtype]
  crux.tx.consumer/PolledEventLog

  (new-event-log-context [this]
    (reify Closeable
      (close [_])))

  (next-events [this context next-offset]
    (mapv (partial event-result->message dbtype)
          (jdbc/execute! ds
                         ["SELECT EVENT_OFFSET, EVENT_KEY, TX_TIME, V, TOPIC FROM tx_events WHERE EVENT_OFFSET >= ? ORDER BY EVENT_OFFSET" next-offset]
                         {:max-rows 100 :builder-fn jdbcr/as-unqualified-lower-maps})))

  (end-offset [this]
    (inc (val (first (jdbc/execute-one! ds ["SELECT max(EVENT_OFFSET) FROM tx_events"]))))))

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
  (p/start-event-log-consumer indexer (JDBCEventLogConsumer. ds dbtype)))

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
                                         :deps [::ds]}}))
