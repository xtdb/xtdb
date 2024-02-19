(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           [java.nio.file Path]
           [java.util Comparator LinkedList PriorityQueue]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb Compactor IBufferPool)
           xtdb.bitemporal.IPolygonReader
           (xtdb.trie EventRowPointer IDataRel)
           xtdb.vector.IRelationWriter
           xtdb.vector.IRowCopier
           xtdb.vector.IVectorWriter
           xtdb.vector.RelationReader))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll []))

(defn- ->reader->copier [^IRelationWriter data-wtr]
  (let [iid-wtr (.colWriter data-wtr "xt$iid")
        sf-wtr (.colWriter data-wtr "xt$system_from")
        vf-wtr (.colWriter data-wtr "xt$valid_from")
        vt-wtr (.colWriter data-wtr "xt$valid_to")
        op-wtr (.colWriter data-wtr "op")]
    (fn reader->copier [^RelationReader data-rdr]
      (let [iid-copier (-> (.readerForName data-rdr "xt$iid") (.rowCopier iid-wtr))
            sf-copier (-> (.readerForName data-rdr "xt$system_from") (.rowCopier sf-wtr))
            vf-copier (-> (.readerForName data-rdr "xt$valid_from") (.rowCopier vf-wtr))
            vt-copier (-> (.readerForName data-rdr "xt$valid_to") (.rowCopier vt-wtr))
            op-copier (-> (.readerForName data-rdr "op") (.rowCopier op-wtr))]
        (reify IRowCopier
          (copyRow [_ ev-idx]
            (.startRow data-wtr)
            (let [pos (.copyRow iid-copier ev-idx)]
              (.copyRow sf-copier ev-idx)
              (.copyRow vf-copier ev-idx)
              (.copyRow vt-copier ev-idx)
              (.copyRow op-copier ev-idx)
              (.endRow data-wtr)

              pos)))))))

(defn merge-segments-into [^IRelationWriter data-rel-wtr, ^IVectorWriter recency-wtr, segments]
  (let [reader->copier (->reader->copier data-rel-wtr)
        calculate-polygon (bitemp/polygon-calculator)

        is-valid-ptr (ArrowBufPointer.)]

    (doseq [{:keys [path segments nodes]} (trie/->merge-plan segments)
            :let [data-rdrs (trie/load-data-pages (map :data-rel segments) nodes)
                  merge-q (PriorityQueue. (Comparator/comparing (util/->jfn :ev-ptr) (EventRowPointer/comparator)))]]

      (doseq [^RelationReader data-rdr data-rdrs
              :when data-rdr
              :let [ev-ptr (EventRowPointer. data-rdr path)
                    row-copier (reader->copier data-rdr)]]
        (when (.isValid ev-ptr is-valid-ptr path)
          (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

      (loop []
        (when-let [{:keys [^EventRowPointer ev-ptr, ^IRowCopier row-copier] :as q-obj} (.poll merge-q)]
          (.copyRow row-copier (.getIndex ev-ptr))

          (.writeLong recency-wtr
                      (.getRecency ^IPolygonReader (calculate-polygon ev-ptr)))

          (.nextIndex ev-ptr)
          (when (.isValid ev-ptr is-valid-ptr path)
            (.add merge-q q-obj))
          (recur))))

    nil))

(defn ->log-data-rel-schema [data-rels]
  (trie/data-rel-schema (->> (for [^IDataRel data-rel data-rels]
                               (-> (.getSchema data-rel)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn open-recency-wtr [allocator]
  (vw/->vec-writer allocator "xt$recency"
                   (FieldType/notNullable #xt.arrow/type [:timestamp-tz :micro "UTC"])))

(defn exec-compaction-job! [^BufferAllocator allocator, ^IBufferPool buffer-pool, {:keys [page-size]}
                            {:keys [^Path table-path trie-keys out-trie-key]}]
  (try
    (log/infof "compacting '%s' '%s' -> '%s'..." table-path trie-keys out-trie-key)

    (util/with-open [meta-files (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-path trie-keys nil)]
      (doseq [trie-key trie-keys]
        (.add meta-files (trie/open-meta-file buffer-pool (trie/->table-meta-file-path table-path trie-key))))

      (let [segments (mapv (fn [{:keys [trie] :as meta-file} data-rel]
                             {:trie trie, :meta-file meta-file, :data-rel data-rel})
                           meta-files
                           data-rels)
            schema (->log-data-rel-schema (map :data-rel segments))]

        (util/with-open [data-rel-wtr (trie/open-log-data-wtr allocator schema)
                         recency-wtr (open-recency-wtr allocator)]
          (merge-segments-into data-rel-wtr recency-wtr segments)

          (util/with-open [trie-wtr (trie/open-trie-writer allocator buffer-pool
                                                           schema table-path out-trie-key)]

            (Compactor/writeRelation trie-wtr (vw/rel-wtr->rdr data-rel-wtr) (vw/vec-wtr->rdr recency-wtr) page-size)))))

    (log/infof "compacted '%s' -> '%s'." table-path out-trie-key)

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn- l0->l1-compaction-job [l0-files l1-files {:keys [^long l1-file-size-rows]}]
  ;; if there are current L0 files, merge them into the latest l1 file until it's full
  (when l0-files
    (let [{:keys [trie-keys next-row rows]}
          (reduce (fn [{:keys [rows trie-keys]}
                       {l0-rows :rows, l0-trie-key :trie-key, :keys [next-row]}]
                    (let [new-rows (+ rows l0-rows)]
                      (cond-> {:rows new-rows
                               :trie-keys (conj trie-keys l0-trie-key)
                               :next-row next-row}
                        (>= new-rows l1-file-size-rows) reduced)))

                  (let [{:keys [rows trie-key] :as last-l1} (last l1-files)]
                    (if (or (nil? last-l1)
                            (>= rows l1-file-size-rows))
                      {:rows 0, :trie-keys []}
                      {:rows rows, :trie-keys [trie-key]}))

                  l0-files)]

      {:trie-keys trie-keys
       :out-trie-key (trie/->log-l0-l1-trie-key 1 next-row rows)})))

(defn compaction-jobs [meta-file-names opts]
  (let [{l0-files 0, l1-files 1} (->> (trie/current-trie-files meta-file-names)
                                      (map trie/parse-trie-file-path)
                                      (group-by :level))]

    (when l0-files
      [(l0->l1-compaction-job l0-files l1-files opts)])))

(defmethod ig/prep-key :xtdb/compactor [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :buffer-pool (ig/ref :xtdb/buffer-pool)}
        opts))

(def ^:dynamic *page-size* 1024)
(def ^:dynamic *l1-file-size-rows* (bit-shift-left 1 18))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [allocator ^IBufferPool buffer-pool]}]
  (let [page-size *page-size*
        l1-file-size-rows *l1-file-size-rows*]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
      (reify ICompactor
        (compactAll [_]
          (log/info "compact-all")

          (loop []
            (let [jobs (for [table-path (.listObjects buffer-pool util/tables-dir)
                             job (compaction-jobs (trie/list-meta-files buffer-pool table-path)
                                                  {:l1-file-size-rows l1-file-size-rows})]
                         (assoc job :table-path table-path))
                  jobs? (boolean (seq jobs))]

              (doseq [job jobs]
                (exec-compaction-job! allocator buffer-pool {:page-size page-size} job))

              (when jobs?
                (recur)))))
        AutoCloseable
        (close [_]
          (util/close allocator))))))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all! [node]
  (let [^ICompactor compactor (util/component node :xtdb/compactor)]
    (.compactAll compactor)))
