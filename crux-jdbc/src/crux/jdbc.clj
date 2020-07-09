(ns crux.jdbc
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.node :as n]
            [crux.tx :as tx]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as jdbcc]
            [next.jdbc.result-set :as jdbcr]
            [taoensso.nippy :as nippy]
            [clojure.string :as str]
            [crux.io :as cio]
            [crux.codec :as c])
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
    (doseq [[id doc] id-and-docs
            :let [id (str id)]]
      (if (c/evicted-doc? doc)
        (do
          (insert-event! ds id doc "docs")
          (evict-doc! ds id doc))
        (if-not (doc-exists? ds id)
          (insert-event! ds id doc "docs")
          (update-doc! ds id doc)))))

  (fetch-docs [this ids]
    (->> (for [id-batch (partition-all 100 ids)
               row (jdbc/execute! ds (into [(format "SELECT EVENT_KEY, V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY IN (%s) AND COMPACTED = 0"
                                                    (->> (repeat (count id-batch) "?") (str/join ", ")))]
                                           (map str id-batch))
                                  {:builder-fn jdbcr/as-unqualified-lower-maps})]
           row)
         (map (juxt (comp c/new-id :event_key) #(->v dbtype (:v %))))
         (into {}))))

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
    (when-let [max-offset (-> (jdbc/execute-one! ds ["SELECT max(EVENT_OFFSET) AS max_offset FROM tx_events"]
                                                 {:builder-fn jdbcr/as-unqualified-lower-maps})
                              :max_offset)]
      {:crux.tx/tx-id max-offset})))

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

(def topology
  (merge n/base-topology
         {::ds {:start-fn start-jdbc-ds
                :args {::dbtype {:doc "Database type"
                                 :required? true
                                 :crux.config/type :crux.config/string}
                       ::dbname {:doc "Database name"
                                 :required? true
                                 :crux.config/type :crux.config/string}}}

          ::n/tx-log {:start-fn (fn [{::keys [ds]} {::keys [dbtype]}]
                                  (->JdbcTxLog ds dbtype))
                      :deps [::ds]}

          ::n/document-store {:start-fn (fn [{::keys [ds]} {::keys [dbtype]}]
                                          (->JdbcDocumentStore ds dbtype))
                              :deps [::ds]}}))
