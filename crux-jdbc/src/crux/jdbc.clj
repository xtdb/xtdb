(ns crux.jdbc
  (:require [clojure.core.reducers :as r]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            crux.api
            [crux.bootstrap :as b]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.tx :as tx]
            crux.tx.consumer
            [crux.tx.polling :as p]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]
            [taoensso.nippy :as nippy])
  (:import crux.tx.consumer.Message
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

(defmulti ->date (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->date :default [_ t]
  (assert t)
  (java.util.Date. (.getTime ^java.sql.Timestamp t)))

(defmulti ->v (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->v :default [_ v] (nippy/thaw v))

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

    (println "here" tx-result)
    (let [tx-id (:event_offset tx-result)
          tx-time (:tx_time tx-result)]
      (Tx. (->date dbtype tx-time) tx-id))))

(defn- insert-event! [ds event-key v topic]
  (let [b (nippy/freeze v)]
    (jdbc/execute-one! ds ["INSERT INTO tx_events (EVENT_KEY, V, TOPIC) VALUES (?,?,?)" event-key b topic]
                       {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})))

(defrecord JdbcLogQueryContext [running?]
  Closeable
  (close [_]
    (reset! running? false)))

(defrecord JdbcTxLog [ds dbtype]
  db/TxLog
  (submit-doc [this content-hash doc]
    (let [id (str content-hash)
          ^Tx result (tx-result->tx-data ds dbtype (insert-event! ds id  doc "docs"))]
      (jdbc/execute! ds ["DELETE FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY = ? AND EVENT_OFFSET < ?" id (.id result)])))

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
          running? (:running? tx-log-context)]
      (future
        (try
          (r/reduce (fn [_ y]
                      (.put q {:crux.tx/tx-id (:event_offset y)
                               :crux.tx/tx-time (->date dbtype (:tx_time y))
                               :crux.api/tx-ops (nippy/thaw (:v y))})
                      (if @running?
                        nil
                        (reduced nil)))
                    nil
                    (jdbc/plan ds ["SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET >= ?"
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

(defrecord JDBCEventLogConsumer [ds dbtype]
  crux.tx.consumer/PolledEventLog

  (new-event-log-context [this]
    (reify Closeable
      (close [_])))

  (next-events [this context next-offset]
    (jdbc/with-transaction [t ds]
      (doall
       (map (fn [result]
              (Message. (->v dbtype (:v result))
                        nil
                        (:event_offset result)
                        (->date dbtype (:tx_time result))
                        (:event_key result)
                        {:crux.tx/sub-topic (keyword (:topic result))}))
            (jdbc/execute! t ["SELECT EVENT_OFFSET, EVENT_KEY, TX_TIME, V, TOPIC FROM tx_events WHERE EVENT_OFFSET >= ?" next-offset]
                           {:max-rows 10 :builder-fn jdbcr/as-unqualified-lower-maps})))))

  (end-offset [this]
    (inc (val (first (jdbc/execute-one! ds ["SELECT max(EVENT_OFFSET) FROM tx_events"]))))))

(defn- start-jdbc-ds [_ {:keys [dbtype] :as options}]
  (require (symbol (str "crux.jdbc." (name (dbtype->crux-jdbc-dialect dbtype)))))
  (let [ds (jdbc/get-datasource options)]
    (setup-schema! dbtype ds)
    ds))

(defn- start-tx-log [{:keys [ds]} {:keys [dbtype]}]
  (map->JdbcTxLog {:ds ds :dbtype dbtype}))

(defn- start-event-log-consumer [{:keys [indexer ds]} {:keys [dbtype]}]
  (p/start-event-log-consumer indexer (JDBCEventLogConsumer. ds dbtype)))

(def ds [start-jdbc-ds [] (s/keys :req-un [::dbtype ::dbname])])
(def tx-log [start-tx-log [:ds]])
(def event-log-consumer [start-event-log-consumer [:indexer :ds]])

(def node-config {:ds ds
                  :tx-log tx-log
                  :event-log-consumer event-log-consumer})

(comment
;; Start a JDBC node:
(b/start-node node-config some-options))
