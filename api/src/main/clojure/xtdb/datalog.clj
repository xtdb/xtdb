(ns xtdb.datalog
  (:require xtdb.api
            [xtdb.api.impl :as impl])
  (:import xtdb.IResultSet
           java.util.function.Function))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q& ^java.util.concurrent.CompletableFuture [node q & args]
  (-> (impl/open-datalog& node (-> q (update :basis impl/after-latest-submitted-tx node)) args)
      (.thenApply
       (reify Function
         (apply [_ res]
           (with-open [^IResultSet res res]
             (vec (iterator-seq res))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q [node q & args]
  (-> @(apply q& node q args)
      (impl/rethrowing-cause)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts] (impl/submit-tx& node tx-ops tx-opts)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  (^xtdb.api.TransactionInstant [node tx-ops] (submit-tx node tx-ops {}))
  (^xtdb.api.TransactionInstant [node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status [node]
  (impl/status node))
