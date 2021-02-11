(ns core2.tx
  (:import [java.util Date Map]))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(defrecord Watermark [^long chunk-idx ^Map chunk-object-key->idx])
