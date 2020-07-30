(ns crux.jdbc
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.document-store :as ds]
            [crux.lru :as lru]
            [crux.node :as n]
            [crux.tx :as tx]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as jdbcc]
            [next.jdbc.result-set :as jdbcr]
            [taoensso.nippy :as nippy]
            [clojure.string :as str]
            [crux.io :as cio]
            [crux.codec :as c]
            [crux.api :as api]
            [crux.system :as sys])
  (:import com.zaxxer.hikari.HikariDataSource
           [java.util.concurrent LinkedBlockingQueue TimeUnit]
           java.util.Date))

(defn- dbtype->crux-jdbc-dialect [dbtype]
  (condp contains? dbtype
    #{"h2"} :h2
    #{"mysql"} :mysql
    #{"mssql"} :mssql
    #{"sqlite"} :sqlite
    #{"postgresql" "pgsql"} :psql
    #{"oracle"} :oracle))

(defmulti ->date (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->date :default [_ t]
  (assert t)
  (java.util.Date. (.getTime ^java.sql.Timestamp t)))

(defmulti ->v (fn [dbtype d] (dbtype->crux-jdbc-dialect dbtype)))

(defmethod ->v :default [_ v] (nippy/thaw v))

(defn ->open-data-source [opts]
  (fn [db-spec]
    (jdbcc/->pool HikariDataSource (merge opts db-spec))))

(deftype Tx [^Date time ^long id])

(defn- tx-result->tx-data [ds dbtype tx-result]
  (let [tx-result (condp contains? dbtype
                    #{"sqlite" "mysql"}
                    (let [id (first (vals tx-result))]
                      (jdbc/execute-one! ds ["SELECT * FROM tx_events WHERE EVENT_OFFSET = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps}))

                    #{"mssql"}
                    (if-let [id (:generated_keys tx-result)]
                      (jdbc/execute-one! ds ["SELECT * FROM tx_events WHERE EVENT_OFFSET = ?" id]
                                         {:return-keys true :builder-fn jdbcr/as-unqualified-lower-maps})
                      tx-result)

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

(defn- update-doc! [ds k doc]
  (jdbc/execute! ds ["UPDATE tx_events SET V = ? WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze doc) k]))

(defn- evict-doc! [ds k tombstone]
  (jdbc/execute! ds ["UPDATE tx_events SET V = ?, COMPACTED = 1 WHERE TOPIC = 'docs' AND EVENT_KEY = ?" (nippy/freeze tombstone) k]))

(defrecord JdbcDocumentStore [ds dbtype]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (jdbc/with-transaction [tx ds]
      (doseq [[id doc] id-and-docs
              :let [id (str id)]]
        (if (c/evicted-doc? doc)
          (do
            (insert-event! tx id doc "docs")
            (evict-doc! tx id doc))
          (if-not (doc-exists? tx id)
            (insert-event! tx id doc "docs")
            (update-doc! tx id doc))))))

  (fetch-docs [this ids]
    (->> (for [id-batch (partition-all 100 ids)
               row (jdbc/execute! ds (into [(format "SELECT EVENT_KEY, V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY IN (%s) AND COMPACTED = 0"
                                                    (->> (repeat (count id-batch) "?") (str/join ", ")))]
                                           (map (comp str c/new-id) id-batch))
                                  {:builder-fn jdbcr/as-unqualified-lower-maps})]
           row)
         (map (juxt (comp c/new-id c/hex->id-buffer :event_key) #(->v dbtype (:v %))))
         (into {}))))

(defn ->document-store {::sys/deps {:data-source nil}
                        ::sys/args {:dbtype {:required? true
                                             :spec ::sys/string}
                                    :doc-cache-size ds/doc-cache-size-opt}}
  [{:keys [data-source dbtype doc-cache-size]}]
  (->> (->JdbcDocumentStore data-source dbtype)
       (ds/->CachedDocumentStore (lru/new-cache doc-cache-size))))

(defrecord JdbcTxLog [ds dbtype]
  db/TxLog
  (submit-tx [this tx-events]
    (let [^Tx tx (tx-result->tx-data ds dbtype (insert-event! ds nil tx-events "txs"))]
      (delay {:crux.tx/tx-id (.id tx)
              :crux.tx/tx-time (.time tx)})))

  (open-tx-log [this after-tx-id]
    (let [conn (jdbc/get-connection ds)
          stmt (jdbc/prepare conn
                             ["SELECT EVENT_OFFSET, TX_TIME, V, TOPIC FROM tx_events WHERE TOPIC = 'txs' and EVENT_OFFSET > ? ORDER BY EVENT_OFFSET"
                              (or after-tx-id 0)])
          rs (.executeQuery stmt)]
      (cio/->cursor #(run! cio/try-close [rs stmt conn])
                    (->> (resultset-seq rs)
                         (map (fn [y]
                                {:crux.tx/tx-id (int (:event_offset y))
                                 :crux.tx/tx-time (->date dbtype (:tx_time y))
                                 :crux.tx.event/tx-events (->v dbtype (:v y))}))))))

  (latest-submitted-tx [this]
    (when-let [max-offset (-> (jdbc/execute-one! ds ["SELECT max(EVENT_OFFSET) AS max_offset FROM tx_events WHERE topic = 'txs'"]
                                                 {:builder-fn jdbcr/as-unqualified-lower-maps})
                              :max_offset)]
      {:crux.tx/tx-id max-offset})))

(defn ->tx-log {::sys/deps {:data-source nil}
                ::sys/args {:dbtype {:required? true
                                     :spec ::sys/string}}}
  [{:keys [data-source dbtype]}]
  (->JdbcTxLog data-source dbtype))

(defn ->tx-consumer {::sys/deps (merge (::sys/deps (meta #'tx/->polling-tx-consumer))
                                       {:tx-log :crux/tx-log})
                     ::sys/args (::sys/args (meta #'tx/->polling-tx-consumer))}
  [{:keys [tx-log] :as opts}]
  (tx/->polling-tx-consumer opts
                            (fn [after-tx-id]
                              (db/open-tx-log tx-log after-tx-id))))
