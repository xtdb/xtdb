(ns xtdb.node.impl
  (:require [clojure.pprint :as pp]
            [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.indexer
            [xtdb.log :as log]
            [xtdb.operator.scan :as scan]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.xtql :as xtql])
  (:import (java.io Closeable Writer)
           (java.util.concurrent CompletableFuture)
           java.util.HashMap
           [java.util.stream Stream]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.api IXtdb TransactionKey Xtdb$Config)
           (xtdb.api.log Log)
           xtdb.api.module.XtdbModule$Factory
           (xtdb.api.query Basis IKeyFn QueryOptions XtqlQuery)
           xtdb.indexer.IIndexer
           (xtdb.query IRaQuerySource)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/init-key :xtdb/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :xtdb/allocator [_ ^BufferAllocator a]
  (util/close a))

(defmethod ig/init-key :xtdb/default-tz [_ default-tz] default-tz)

(defn- with-after-tx-default [opts]
  (-> opts
      (update :after-tx time/max-tx (get-in opts [:basis :at-tx]))))

(defrecord Node [^BufferAllocator allocator
                 ^IIndexer indexer
                 ^Log log
                 ^IRaQuerySource ra-src, wm-src, scan-emitter
                 default-tz
                 !latest-submitted-tx
                 system, close-fn]
  IXtdb
  (submitTxAsync [this opts tx-ops]
    (let [system-time (some-> opts .getSystemTime)]
      (-> (log/submit-tx& this (vec tx-ops) opts)
          (util/then-apply
            (fn [^TransactionKey tx-key]
              (let [tx-key (cond-> tx-key
                             system-time (.withSystemTime system-time))]

                (swap! !latest-submitted-tx time/max-tx tx-key)
                tx-key))))))

  (^CompletableFuture openQueryAsync [_ ^String query, ^QueryOptions query-opts]
   (let [query-opts (-> (into {:default-tz default-tz,
                               :after-tx @!latest-submitted-tx
                               :key-fn #xt/key-fn :snake-case-string}
                              query-opts)
                        (update :basis (fn [b] (cond->> b (instance? Basis b) (into {}))))
                        (with-after-tx-default))]
     (-> (.awaitTxAsync indexer
                        (-> (:after-tx query-opts)
                            (time/max-tx (get-in query-opts [:basis :at-tx])))
                        (:tx-timeout query-opts))
         (util/then-apply
           (fn [_]
             (let [table-info (scan/tables-with-cols query-opts wm-src scan-emitter)
                   plan (sql/compile-query query (-> query-opts (assoc :table-info table-info)))]
               (if (:explain? query-opts)
                 (Stream/of {(.denormalize ^IKeyFn (:key-fn query-opts) "plan") plan})

                 (q/open-query allocator ra-src wm-src plan query-opts))))))))

  (^CompletableFuture openQueryAsync [_ ^XtqlQuery query, ^QueryOptions query-opts]
   (let [query-opts (-> (into {:default-tz default-tz,
                               :after-tx @!latest-submitted-tx
                               :key-fn #xt/key-fn :camel-case-string}
                              query-opts)
                        (update :basis (fn [b] (cond->> b (instance? Basis b) (into {}))))
                        (with-after-tx-default))]
     (-> (.awaitTxAsync indexer
                        (-> (:after-tx query-opts)
                            (time/max-tx (get-in query-opts [:basis :at-tx])))
                        (:tx-timeout query-opts))
         (util/then-apply
           (fn [_]
             (let [table-info (scan/tables-with-cols query-opts wm-src scan-emitter)
                   plan (xtql/compile-query query table-info)]
               (if (:explain? query-opts)
                 (Stream/of {(.denormalize ^IKeyFn (:key-fn query-opts) "plan") plan})

                 (q/open-query allocator ra-src wm-src plan query-opts))))))))

  xtp/PStatus
  (latest-submitted-tx [_] @!latest-submitted-tx)
  (status [this]
    {:latest-completed-tx (.latestCompletedTx indexer)
     :latest-submitted-tx (xtp/latest-submitted-tx this)})

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key :xtdb/node [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :indexer (ig/ref :xtdb/indexer)
          :wm-src (ig/ref :xtdb/indexer)
          :log (ig/ref :xtdb/log)
          :default-tz (ig/ref :xtdb/default-tz)
          :ra-src (ig/ref :xtdb.query/ra-query-source)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)}
         opts))

(defmethod ig/init-key :xtdb/node [_ deps]
  (map->Node (-> deps
                 (assoc :!latest-submitted-tx (atom nil)))))

(defmethod ig/halt-key! :xtdb/node [_ node]
  (util/try-close node))

(defmethod ig/prep-key :xtdb/modules [_ modules]
  {:node (ig/ref :xtdb/node)
   :modules (vec modules)})

(defmethod ig/init-key :xtdb/modules [_ {:keys [node modules]}]
  (util/with-close-on-catch [!started-modules (HashMap. (count modules))]
    (doseq [^XtdbModule$Factory module modules]
      (.put !started-modules (.getModuleKey module) (.openModule module node)))

    (into {} !started-modules)))

(defmethod ig/halt-key! :xtdb/modules [_ modules]
  (util/close modules))

(defn node-system [^Xtdb$Config opts]
  (-> {:xtdb/node {}
       :xtdb/allocator {}
       :xtdb/indexer {}
       :xtdb.log/watcher {}
       :xtdb.metadata/metadata-manager {}
       :xtdb.operator.scan/scan-emitter {}
       :xtdb.query/ra-query-source {}
       :xtdb/compactor {}

       :xtdb/buffer-pool (.getStorage opts)
       :xtdb.indexer/live-index (.indexer opts)
       :xtdb/log (.getTxLog opts)
       :xtdb/modules (.getModules opts)
       :xtdb/default-tz (.getDefaultTz opts)
       :xtdb.stagnant-log-flusher/flusher (.indexer opts)}
      (doto ig/load-namespaces)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-node ^xtdb.api.IXtdb [opts]
  (let [!closing (atom false)
        system (-> (node-system opts)
                   ig/prep
                   ig/init)]

    (-> (:xtdb/node system)
        (assoc :system system
               :close-fn #(when (compare-and-set! !closing false true)
                            (ig/halt! system)
                            #_(println (.toVerboseString ^RootAllocator (:xtdb/allocator system))))))))
