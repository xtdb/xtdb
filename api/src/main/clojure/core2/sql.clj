(ns core2.sql
  (:require core2.api
            [core2.api.impl :as impl])
  (:import core2.IResultSet
           java.util.function.Function))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q& ^java.util.concurrent.CompletableFuture
  ([node q] (q& node q {}))

  ([node q opts]
   (-> (impl/open-sql& node q (-> opts (update :basis impl/after-latest-submitted-tx node)))
       (.thenApply
        (reify Function
          (apply [_ res]
            (with-open [^IResultSet res res]
              (vec (iterator-seq res)))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn q
  ([node sql] (q node sql {}))
  ([node sql opts]
   (-> @(q& node sql opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx&
  (^java.util.concurrent.CompletableFuture [node tx-ops] (submit-tx& node tx-ops {}))
  (^java.util.concurrent.CompletableFuture [node tx-ops tx-opts] (impl/submit-tx& node tx-ops tx-opts)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-tx
  (^core2.api.TransactionInstant [node tx-ops] (submit-tx node tx-ops {}))
  (^core2.api.TransactionInstant [node tx-ops tx-opts]
   (-> @(submit-tx& node tx-ops tx-opts)
       (impl/rethrowing-cause))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn status [node]
  (impl/status node))
