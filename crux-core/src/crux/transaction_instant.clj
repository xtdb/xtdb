(ns ^:no-doc crux.transaction-instant
  (:import java.io.Writer))

(defrecord TransactionInstant []
  crux.api.TransactionInstant
  (getTxId [this] (:crux.tx/tx-id this))
  (getTxTime [this] (:crux.tx/tx-time this)))

(defmethod print-method TransactionInstant [ti ^Writer w]
  (.write w "#crux/transaction-instant ")
  (print-method (into {} ti) w))

(defn ->transaction-instant
  ([tx-map]
   (some-> tx-map map->TransactionInstant))
  ([tx-id tx-time]
   (-> {:crux.tx/tx-id tx-id :crux.tx/tx-time tx-time} map->TransactionInstant)))
