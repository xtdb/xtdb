(ns xtdb.migration
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.migration.v05 :as v05]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.api.storage Storage Storage$Factory]
           xtdb.BufferPool))

(defmethod ig/prep-key ::source [_ {:keys [factory storage-version], :or {storage-version Storage/VERSION}}]
  {:allocator (ig/ref :xtdb/allocator)
   :factory factory
   :storage-version storage-version
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key ::source [_ {:keys [allocator ^Storage$Factory factory, metrics-registry, ^long storage-version]}]
  (.open factory allocator metrics-registry storage-version))

(defmethod ig/halt-key! ::source [_ ^BufferPool buffer-pool]
  (util/close buffer-pool))

(defn migration-system [^Xtdb$Config opts, ^long from-version]
  (-> {::source {:factory (.getStorage opts), :storage-version from-version}

       :xtdb/config opts
       :xtdb/allocator {}
       :xtdb.metrics/registry {}
       :xtdb/buffer-pool (.getStorage opts)
       :xtdb/block-catalog {}
       :xtdb/table-catalog {}
       :xtdb/trie-catalog {}}

      (doto ig/load-namespaces)))

(defn migrate-from
  ([^long from-version node-opts] (migrate-from from-version node-opts {}))

  ([^long from-version node-opts {:keys [force?]}]
   (log/infof "Starting migration tool: from storage version %d to %d" from-version Storage/VERSION)

   (let [{^BufferPool bp :xtdb/buffer-pool, :as system} (-> (migration-system (xtn/->config node-opts) from-version)
                                                            ig/prep
                                                            ig/init)]
     (try
       (let [existing-objs? (boolean (seq (.listAllObjects bp)))]
         (if (and existing-objs? (not force?))
           (do
             (log/error "Existing objects in the target directory - use the `--force` flag to clear the target directory.")
             1)

           (do
             (when (and existing-objs? force?)
               (log/info "`--force` provided: clearing existing objects in the target directory.")
               (.deleteAllObjects bp))

             (assert (empty? (.listAllObjects bp)) "Target directory not empty.")

             (case from-version
               5 (v05/migrate->v06! system)
               (throw (err/illegal-arg :unsupported-migration-version
                                       {::err/message (format "Unsupported migration version: %d" from-version)})))

             (log/info "\nThe migration is complete, and this task will now exit.\nYou may now upgrade your new XTDB nodes in the usual green/blue manner.")

             0)))

       (finally
         (ig/halt! system))))))
