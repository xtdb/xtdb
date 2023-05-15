(ns xtdb.api.protocols)

(defprotocol PNode
  (^java.util.concurrent.CompletableFuture open-datalog& [node q opts])
  (^java.util.concurrent.CompletableFuture open-sql& [node q opts])
  (^xtdb.api.TransactionInstant latest-submitted-tx [node]))

(defprotocol PSubmitNode
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> submit-tx&
   [node tx-ops]
   [node tx-ops opts]))

(defprotocol PStatus
  (status [node]))

(defn max-tx [l r]
  (if (or (nil? l)
          (and r (neg? (compare l r))))
    r
    l))

(defn after-latest-submitted-tx [basis node]
  (cond-> basis
    (not (or (contains? basis :tx)
             (contains? basis :after-tx)))
    (assoc :after-tx (latest-submitted-tx node))))
