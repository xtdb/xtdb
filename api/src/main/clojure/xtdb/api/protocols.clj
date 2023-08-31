(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false} xtdb.api.protocols
  (:import java.io.Writer
           java.time.Instant
           xtdb.types.ClojureForm))

(defrecord TransactionInstant [^long tx-id, ^Instant system-time]
  Comparable
  (compareTo [_ tx-key]
    (Long/compare tx-id (.tx-id ^TransactionInstant tx-key))))

(defmethod print-dup TransactionInstant [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TransactionInstant [tx-key w]
  (print-dup tx-key w))

(defn ->ClojureForm [form]
  (ClojureForm. form))

(defmethod print-dup ClojureForm [^ClojureForm clj-form ^Writer w]
  (.write w "#xt/clj-form ")
  (print-method (.form clj-form) w))

(defmethod print-method ClojureForm [clj-form w]
  (print-dup clj-form w))

(defprotocol PNode
  (^java.util.concurrent.CompletableFuture open-query& [node query opts])
  (^xtdb.api.protocols.TransactionInstant latest-submitted-tx [node]))

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

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/query" {:name :query
              :summary "Query"}]

   ["/swagger.json" {:name :swagger-json
                     :no-doc true}]])
