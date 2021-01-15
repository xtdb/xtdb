(ns crux.history-options)

(defrecord HistoryOptions [with-corrections? with-docs?
                           start-valid-time start-tx
                           end-valid-time end-tx]
  crux.api.HistoryOptions
  (withCorrections [this with-corrections?] (assoc this :with-corrections? with-corrections?))
  (withDocs [this with-docs?] (assoc this :with-docs? with-docs?))
  (startValidTime [this start-valid-time] (assoc this :start-valid-time start-valid-time))
  (startTransaction [this start-tx] (assoc this :start-tx start-tx))
  (startTransactionTime [this start-tx-time] (assoc this :start-tx {:crux.tx/tx-time start-tx-time}))
  (endValidTime [this end-valid-time] (assoc this :end-valid-time end-valid-time))
  (endTransaction [this end-tx] (assoc this :end-tx end-tx))
  (endTransactionTime [this end-tx-time] (assoc this :end-tx {:crux.tx/tx-time end-tx-time})))

(defn ->history-options
  ([] (->history-options {}))

  ([{:keys [with-corrections? with-docs? start end]
     :or {with-corrections? false, with-docs? false}}]
   (->HistoryOptions (boolean with-corrections?)
                     (boolean with-docs?)
                     (:crux.db/valid-time start)
                     (not-empty (select-keys start [:crux.tx/tx-time :crux.tx/tx-id]))
                     (:crux.db/valid-time end)
                     (not-empty (select-keys end [:crux.tx/tx-time :crux.tx/tx-id])))))
