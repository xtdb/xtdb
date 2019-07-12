(ns crux.jdbc
  (:require [next.jdbc :as jdbc]
            [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [crux.db :as db]
            crux.api
            [crux.tx :as tx]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [clojure.core.reducers :as r])
  (:import java.io.Closeable
           java.util.Date
           java.util.concurrent.LinkedBlockingQueue
           java.util.concurrent.TimeUnit))

(deftype Tx [^Date time ^long id])

(defn- ->date [^java.sql.Timestamp t]
  (java.util.Date. (.getTime t)))

(defn- tx-result->tx-data [ds tx-result]
  (let [tx-id (get tx-result (keyword "SCOPE_IDENTITY()"))
        tx-time (:TX_EVENTS/TX_TIME (first (jdbc/execute! ds ["SELECT TX_TIME FROM TX_EVENTS WHERE OFFSET = ?" tx-id])))]
    (Tx. (->date tx-time) tx-id)))

(defn- insert-event! [ds id v topic]
  (let [b (nippy/freeze v)]
    (jdbc/execute-one! ds ["INSERT INTO TX_EVENTS (ID, V, TOPIC) VALUES (?,?,?)" id b topic] {:return-keys true})))

(defn- next-events [ds next-offset]
  (jdbc/execute! ds ["SELECT OFFSET, ID, TX_TIME, V, TOPIC FROM TX_EVENTS WHERE OFFSET >= ?" next-offset] {:max-rows 10}))

(defn- max-tx-id [ds]
  (val (first (jdbc/execute-one! ds ["SELECT max(OFFSET) FROM TX_EVENTS"]))))

(defn- event-log-consumer-main-loop [{:keys [running? ds indexer batch-size idle-sleep-ms]
                                      :or {batch-size 100
                                           idle-sleep-ms 100}}]
  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {::event-log {:lag 0
                                               :next-offset 0
                                               :time nil}}))
  (while @running?
    (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [::event-log :next-offset])]
      (when-let [last-message (reduce (fn [last-message result]
                                        (case (:TX_EVENTS/TOPIC result)
                                          "tx"
                                          (db/index-tx indexer
                                                       (nippy/thaw (:TX_EVENTS/V result))
                                                       (:TX_EVENTS/TX_TIME result)
                                                       (:TX_EVENTS/OFFSET result))
                                          "doc"
                                          (db/index-doc indexer
                                                        (:TX_EVENTS/ID result)
                                                        (nippy/thaw (:TX_EVENTS/V result))))
                                        result)
                                      nil
                                      (next-events ds next-offset))]

        (let [end-offset (inc (max-tx-id ds))
              next-offset (inc (long (:TX_EVENTS/OFFSET last-message)))
              lag (- end-offset next-offset)
              _ (when (pos? lag)
                  (log/debug "Falling behind" ::event-log "at:" next-offset "end:" end-offset))
              consumer-state {::event-log
                              {:lag lag
                               :next-offset next-offset
                               :time (:TX_EVENTS/TX_TIME last-message)}}]
          (log/debug "Event log consumer state:" (pr-str consumer-state))
          (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state))))
    (Thread/sleep idle-sleep-ms)))

(defn start-event-log-consumer ^java.io.Closeable [ds indexer]
  (let [running? (atom true)
        worker-thread (doto (Thread. #(try
                                        (event-log-consumer-main-loop {:running? running?
                                                                       :ds ds
                                                                       :indexer indexer})
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

(defrecord JdbcLogQueryContext [running?]
  Closeable
  (close [_]
    (reset! running? false)))

(defrecord JdbcTxLog [ds]
  db/TxLog
  (submit-doc [this content-hash doc]
    (insert-event! ds (str content-hash) doc "doc"))

  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (doseq [doc (tx/tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx/tx-ops->tx-events tx-ops)
          ^Tx tx (tx-result->tx-data ds (insert-event! ds nil tx-events "tx"))]
      (delay {:crux.tx/tx-id (.id tx)
              :crux.tx/tx-time (.time tx)})))

  (new-tx-log-context [this]
    (JdbcLogQueryContext. (atom true)))

  (tx-log [this tx-log-context from-tx-id]
    (let [^LinkedBlockingQueue q (LinkedBlockingQueue. 1000)
          running? (:running? tx-log-context)]
      (future
        (r/reduce (fn [_ y]
                    (.put q {:crux.tx/tx-id (:TX_EVENTS/OFFSET y)
                             :crux.tx/tx-time (->date (:TX_EVENTS/TX_TIME y))
                             :crux.api/tx-ops (nippy/thaw (:TX_EVENTS/V y))})
                    (if @running?
                      nil
                      (reduced nil)))
                  nil
                  (jdbc/plan ds ["SELECT OFFSET, ID, TX_TIME, V, TOPIC FROM TX_EVENTS WHERE TOPIC = 'tx' and OFFSET >= ?" (or from-tx-id 0)]))
        (.put q -1))
      ((fn step []
         (lazy-seq
          (when-let [x (.poll q 10000 TimeUnit/MILLISECONDS)]
            (when (not= -1 x)
              (cons x (step))))))))))
