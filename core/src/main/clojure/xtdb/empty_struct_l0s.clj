(ns xtdb.empty-struct-l0s
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.table :as table]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import [java.io ByteArrayOutputStream]
           [java.nio ByteBuffer]
           [java.nio.channels Channels]
           [java.util List]
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector.types.pojo Field Schema)
           (xtdb.api Xtdb$Config)
           (xtdb.arrow ArrowFileLoader ArrowUnloader DenseUnionVector NullVector Relation Vector VectorReader)
           xtdb.BufferPool
           [xtdb.database Database$Config]
           (xtdb.trie Trie TrieCatalog)))

(defmethod ig/init-key ::base [_ deps] deps)

(defn base-system [^Xtdb$Config opts]
  (-> {:xtdb/config opts
       :xtdb/allocator {}
       :xtdb.cache/memory (.getMemoryCache opts)
       :xtdb.cache/disk (.getDiskCache opts)
       :xtdb.metrics/registry {}
       ::base {:allocator (ig/ref :xtdb/allocator)
               :config (ig/ref :xtdb/config)
               :meter-registry (ig/ref :xtdb.metrics/registry)
               :mem-cache (ig/ref :xtdb.cache/memory)
               :disk-cache (ig/ref :xtdb.cache/disk)}}

      (doto ig/load-namespaces)))

(defn- db-system [db-name base ^Database$Config db-config]
  (let [opts {:base base, :db-name db-name}]
    (-> {:xtdb.db-catalog/allocator opts
         :xtdb/block-catalog opts
         :xtdb/table-catalog opts
         :xtdb/trie-catalog opts
         :xtdb/buffer-pool (assoc opts :factory (.getStorage db-config))}
        (doto ig/load-namespaces))))

(defn- affected-schema? [^Schema schema]
  (when-let [put-field (-> (.findField schema "op")
                           (.getChildren)
                           (->> (some #(when (= (.getName ^Field %) "put")
                                         %))))]
    (= put-field (types/->field "put" #xt.arrow/type :struct false))))

(defn migrate-l0s! [node-opts db-name table-name {:keys [dry-run?]}]
  (let [conf (xtn/->config node-opts)
        base-system (-> (base-system conf)
                        ig/prep ig/init)]
    (try
      (let [db-system (-> (db-system db-name (::base base-system)
                                     (or (-> (.getDatabases conf)
                                             (get db-name))
                                         (throw (err/incorrect ::db-not-found "Database not found"
                                                               {:db-name db-name}))))
                          ig/prep ig/init)]
        (try
          (let [^BufferAllocator al (:xtdb/allocator base-system)
                ^BufferPool bp (:xtdb/buffer-pool db-system)
                ^TrieCatalog trie-cat (:xtdb/trie-catalog db-system)
                table (table/->ref table-name)]
            (if dry-run?
              (log/info "Dry run: no changes will be made.")
              (log/infof "Migrating L0s for table '%s' in database '%s'..." table-name db-name))

            (doseq [[_state tries] (-> (trie-cat/trie-state trie-cat table)
                                       (get-in [:tries [0 nil []]]))
                    {:keys [trie-key]} tries
                    :let [data-file-path (Trie/dataFilePath table trie-key)
                          schema (.getSchema (.getFooter bp data-file-path))]]
              (if-not (affected-schema? schema)
                (log/debugf "Trie '%s' not affected" trie-key)

                (let [out-schema (Schema. (for [^Field field (.getFields schema)]
                                            (if (= (.getName field) "op")
                                              (Field. (.getName field) (.getFieldType field)
                                                      (remove #(= (.getName ^Field %) "put") (.getChildren field)))
                                              field)))]
                  (log/infof "Trie '%s' affected" trie-key)
                  (when-not dry-run?
                    (let [data-file-bytes (.getByteArray bp data-file-path)]
                      (log/infof "Backing it up...")
                      (.putObject bp (util/->path (str "l0-backups/" trie-key ".arrow.bak")) (ByteBuffer/wrap data-file-bytes))

                      (with-open [in-rel (Relation. al schema)
                                  loader (ArrowFileLoader/openFromChannel al (util/->seekable-byte-channel (ByteBuffer/wrap data-file-bytes)))
                                  baos (ByteArrayOutputStream.)
                                  out-ch (Channels/newChannel baos)
                                  unloader (ArrowUnloader/open out-ch out-schema)]

                        (dotimes [page-idx (.getPageCount loader)]
                          (util/with-open [rb (.openPage loader page-idx)]
                            (.load in-rel rb)
                            (with-open [out-op-vec (DenseUnionVector. al "op")]
                              (let [in-op-vec (.vectorFor in-rel "op")
                                    delete-wtr (.vectorFor out-op-vec "delete" #xt.arrow/field-type [#xt.arrow/type :null true])
                                    erase-wtr (.vectorFor out-op-vec "erase" #xt.arrow/field-type [#xt.arrow/type :null true])]
                                (dotimes [idx (.getValueCount in-op-vec)]
                                  (case (.getLeg in-op-vec idx)
                                    "delete" (.writeNull delete-wtr)
                                    "erase" (.writeNull erase-wtr))))

                              (let [^List vecs (vec (for [^Vector v (.getVectors in-rel)]
                                                      (if (= (.getName v) "op")
                                                        out-op-vec
                                                        v)))
                                    out-rel (Relation. al vecs (.getRowCount in-rel))]
                                (util/with-open [out-rb (.openArrowRecordBatch out-rel)]
                                  (.writeBatch unloader out-rb))))))

                        (.end unloader)

                        (.putObject bp data-file-path (ByteBuffer/wrap (.toByteArray baos)))))))))

            (when-not dry-run?
              (log/info "L0 files migrated - you can now restart the nodes.")))

          (finally
            (ig/halt! db-system))))

      (finally
        (ig/halt! base-system)))))
