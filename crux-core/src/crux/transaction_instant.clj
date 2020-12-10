(ns ^:no-doc crux.transaction-instant)

(defrecord TransactionInstant [] crux.api.TransactionInstant)

(defn ->transaction-instant [tx-map]
  (some-> tx-map map->TransactionInstant))
