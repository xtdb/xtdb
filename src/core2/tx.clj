(ns core2.tx
  (:import java.util.Date))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])
