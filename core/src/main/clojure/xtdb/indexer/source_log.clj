(ns xtdb.indexer.source-log
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database DatabaseState DatabaseStorage SourceIndexer]
           [xtdb.indexer ReplicaLogProcessor SourceLogProcessor]))

;; Nested integrant sub-system that owns the source indexer's state and subscription.
;; The replica processor is passed in from the parent db-system so we can subscribe
;; to the source log with a SourceLogProcessor passthrough.

(defn- source-system [{:keys [indexer-conf] :as opts}]
  (let [child-opts (dissoc opts :indexer-conf :mode :replica-log)]
    (-> {:xtdb/block-catalog child-opts
         :xtdb/table-catalog child-opts
         :xtdb/trie-catalog child-opts
         :xtdb.indexer/live-index (assoc child-opts :indexer-conf indexer-conf)
         :xtdb.indexer/crash-logger child-opts
         :xtdb.db-catalog/state child-opts}
        (doto ig/load-namespaces))))

(defmethod ig/expand-key :xtdb.indexer/source-log [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/source-log [_ {:keys [^DatabaseStorage db-storage replica-log] :as opts}]
  (let [sys (-> (source-system opts)
                ig/expand
                ig/init)
        ^DatabaseState state (:xtdb.db-catalog/state sys)
        ^ReplicaLogProcessor replica-processor (:xtdb.log/processor (:sys replica-log))
        source-subscription (when replica-processor
                              (let [source-processor (SourceLogProcessor. replica-processor)
                                    source-log (.getSourceLog db-storage)]
                                (.tailAll source-log source-processor (.getLatestProcessedOffset replica-processor))))]
    {:source-indexer (SourceIndexer. nil state)
     :source-subscription source-subscription
     :sys sys}))

(defmethod ig/halt-key! :xtdb.indexer/source-log [_ {:keys [source-subscription sys]}]
  (util/close source-subscription)
  (ig/halt! sys))
