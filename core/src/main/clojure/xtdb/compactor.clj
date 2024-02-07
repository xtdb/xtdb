(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            xtdb.buffer-pool
            xtdb.object-store
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           [java.nio.file Path]
           [java.util Comparator LinkedList PriorityQueue]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot
           xtdb.bitemporal.Polygon
           xtdb.IBufferPool
           (xtdb.trie EventRowPointer IDataRel LiveHashTrie)
           xtdb.util.TemporalBounds
           xtdb.vector.IRelationWriter
           xtdb.vector.IRowCopier
           xtdb.vector.RelationReader))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll []))

(defn- ->log-data-rel-schema [data-rels]
  (trie/data-rel-schema (->> (for [^IDataRel data-rel data-rels]
                               (-> (.getSchema data-rel)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn- ->polygon-writer [^IRelationWriter data-wtr]
  (let [valid-times-wtr (.colWriter data-wtr "xt$valid_times")
        valid-time-wtr (.listElementWriter valid-times-wtr)
        sys-time-ceils-wtr (.colWriter data-wtr "xt$system_time_ceilings")
        sys-time-ceil-wtr (.listElementWriter sys-time-ceils-wtr)]

    (fn write-polygon! [^Polygon polygon]
      (if polygon
        (let [range-count (.getValidTimeRangeCount polygon)]
          (.startList valid-times-wtr)
          (.startList sys-time-ceils-wtr)
          (dotimes [i range-count]
            (.writeLong valid-time-wtr (.getValidFrom polygon i))
            (.writeLong sys-time-ceil-wtr (.getSystemTo polygon i)))

          (.writeLong valid-time-wtr (.getValidTo polygon (dec range-count)))

          (.endList sys-time-ceils-wtr)
          (.endList valid-times-wtr))

        (do
          (.writeNull valid-times-wtr)
          (.writeNull sys-time-ceils-wtr))))))

(defn- ->reader->copier [^IRelationWriter data-wtr]
  (let [iid-wtr (.colWriter data-wtr "xt$iid")
        sf-wtr (.colWriter data-wtr "xt$system_from")
        calculate-polygon (bitemp/polygon-calculator (TemporalBounds.))
        write-polygon! (->polygon-writer data-wtr)
        op-wtr (.colWriter data-wtr "op")]
    (fn reader->copier [^RelationReader data-rdr, ^EventRowPointer ev-ptr]
      (let [iid-copier (-> (.readerForName data-rdr "xt$iid") (.rowCopier iid-wtr))
            sf-copier (-> (.readerForName data-rdr "xt$system_from") (.rowCopier sf-wtr))
            op-copier (-> (.readerForName data-rdr "op") (.rowCopier op-wtr))]
        (reify IRowCopier
          (copyRow [_ ev-idx]
            (.startRow data-wtr)
            (let [pos (.copyRow iid-copier ev-idx)]
              (.copyRow sf-copier ev-idx)

              (doto (calculate-polygon ev-ptr)
                (write-polygon!))

              (.copyRow op-copier ev-idx)
              (.endRow data-wtr)

              pos)))))))

(defn merge-tries! [^BufferAllocator allocator, ^IBufferPool buffer-pool
                    segments table-path trie-key]
  (let [data-rel-schema (->log-data-rel-schema (map :data-rel segments))]
    (util/with-open [trie-wtr (trie/open-trie-writer allocator buffer-pool data-rel-schema table-path trie-key)
                     data-root (VectorSchemaRoot/create data-rel-schema allocator)]
      (let [data-wtr (vw/root->writer data-root)
            reader->copier (->reader->copier data-wtr)
            is-valid-ptr (ArrowBufPointer.)]

        (letfn [(merge-nodes! [path [mn-tag & mn-args]]
                  (case mn-tag
                    :branch (.writeBranch trie-wtr (int-array (first mn-args)))

                    :leaf (let [[segments nodes] mn-args
                                data-rdrs (trie/load-data-pages (map :data-rel segments) nodes)
                                merge-q (PriorityQueue. (Comparator/comparing (util/->jfn :ev-ptr) (EventRowPointer/comparator)))]

                            (doseq [^RelationReader data-rdr data-rdrs
                                    :when data-rdr
                                    :let [ev-ptr (EventRowPointer. data-rdr (byte-array 0))
                                          row-copier (reader->copier data-rdr ev-ptr)]]
                              (when (.isValid ev-ptr is-valid-ptr path)
                                (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

                            (loop [trie (-> (doto (LiveHashTrie/builder (vr/vec->reader (.getVector data-root "xt$iid")))
                                              (.setRootPath path))
                                            (.build))]

                              (if-let [{:keys [^EventRowPointer ev-ptr, ^IRowCopier row-copier] :as q-obj} (.poll merge-q)]
                                (let [ev-idx (.getIndex ev-ptr)
                                      pos (.copyRow row-copier ev-idx)]

                                  (.nextIndex ev-ptr)
                                  (when (.isValid ev-ptr is-valid-ptr path)
                                    (.add merge-q q-obj))
                                  (recur (.add trie pos)))

                                (let [pos (trie/write-live-trie-node trie-wtr (.rootNode (.compactLogs trie)) (vw/rel-wtr->rdr data-wtr))]
                                  (.clear data-root)
                                  (.clear data-wtr)
                                  pos))))))]

          (trie/postwalk-merge-plan segments merge-nodes!)
          (.end trie-wtr))))))

(defn exec-compaction-job! [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                            {:keys [^Path table-path trie-keys out-trie-key]}]
  (try
    (log/infof "compacting '%s' '%s' -> '%s'..." table-path trie-keys out-trie-key)
    (util/with-open [meta-files (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-path trie-keys nil)]
      (doseq [trie-key trie-keys]
        (.add meta-files (trie/open-meta-file buffer-pool (trie/->table-meta-file-path table-path trie-key))))

      (merge-tries! allocator buffer-pool
                    (mapv (fn [{:keys [trie] :as meta-file} data-rel]
                            {:trie trie, :meta-file meta-file, :data-rel data-rel})
                          meta-files
                          data-rels)
                    table-path out-trie-key))

    (log/infof "compacted '%s' -> '%s'." table-path out-trie-key)

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn compaction-jobs [table-path meta-file-names]
  (for [[level parsed-trie-keys] (->> (trie/current-trie-files meta-file-names)
                                      (map trie/parse-trie-file-path)
                                      (group-by :level))
        job (partition 4 parsed-trie-keys)]
    {:table-path table-path
     :trie-keys (mapv :trie-key job)
     :out-trie-key (trie/->log-trie-key (inc level)
                                        (:row-from (first job))
                                        (:next-row (last job)))}))

(defmethod ig/prep-key :xtdb/compactor [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :buffer-pool (ig/ref :xtdb/buffer-pool)}
        opts))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [allocator ^IBufferPool buffer-pool]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (reify ICompactor
      (compactAll [_]
        (log/info "compact-all")
        (loop []
          (let [jobs (for [table-path (.listObjects buffer-pool util/tables-dir)
                           job (compaction-jobs table-path (trie/list-meta-files buffer-pool table-path))]
                       job)
                jobs? (boolean (seq jobs))]

            (doseq [job jobs]
              (exec-compaction-job! allocator buffer-pool job))

            (when jobs?
              (recur)))))
      AutoCloseable
      (close [_]
        (util/close allocator)))))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all! [node]
  (let [^ICompactor compactor (util/component node :xtdb/compactor)]
    (.compactAll compactor)))
