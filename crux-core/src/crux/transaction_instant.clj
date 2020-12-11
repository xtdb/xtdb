(ns ^:no-doc crux.transaction-instant
  (:import java.io.Writer))

(defrecord TransactionInstant [] crux.api.TransactionInstant)

(defmethod print-method TransactionInstant [ti ^Writer w]
  (.write w "#crux/transaction-instant ")
  (print-method (into {} ti) w))

(defn ->transaction-instant [tx-map]
  (some-> tx-map map->TransactionInstant))
