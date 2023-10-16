(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.buffer-pool
            xtdb.object-store
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           [java.util Comparator LinkedList PriorityQueue]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot
           xtdb.IBufferPool
           (xtdb.trie EventRowPointer IDataRel LiveHashTrie)
           xtdb.util.WritableByteBufferChannel
           xtdb.vector.IRowCopier))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll []))

(defn- ->log-data-rel-schema [data-rels]
  (trie/data-rel-schema (->> (for [^IDataRel data-rel data-rels]
                               (-> (.getSchema data-rel)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   (.getChildren) ^Field last
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn merge-tries! [^BufferAllocator allocator, tries, data-rels, data-out-ch, meta-out-ch]
  (let [data-rel-schema (->log-data-rel-schema data-rels)]

    (util/with-open [meta-wtr (trie/open-trie-writer allocator data-rel-schema
                                                     data-out-ch meta-out-ch)

                     data-root (VectorSchemaRoot/create data-rel-schema allocator)]

      (let [data-wtr (vw/root->writer data-root)
            is-valid-ptr (ArrowBufPointer.)]
        (letfn [(merge-nodes! [path [mn-tag mn-arg]]
                  (case mn-tag
                    :branch (.writeBranch meta-wtr (int-array mn-arg))

                    :leaf (let [data-rdrs (trie/load-data-pages data-rels mn-arg)
                                merge-q (PriorityQueue. (Comparator/comparing (util/->jfn :ev-ptr) (EventRowPointer/comparator)))]

                            (doseq [data-rdr data-rdrs
                                    :when data-rdr
                                    :let [ev-ptr (EventRowPointer. data-rdr (byte-array 0))
                                          row-copier (.rowCopier data-wtr data-rdr)]]
                              (when (.isValid ev-ptr is-valid-ptr path)
                                (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

                            (loop [trie (-> (doto (LiveHashTrie/builder (vr/vec->reader (.getVector data-root "xt$iid")))
                                              (.setRootPath path))
                                            (.build))]

                              (if-let [{:keys [^EventRowPointer ev-ptr, ^IRowCopier row-copier] :as q-obj} (.poll merge-q)]
                                (let [pos (.copyRow row-copier (.getIndex ev-ptr))]
                                  (.nextIndex ev-ptr)
                                  (when (.isValid ev-ptr is-valid-ptr path)
                                    (.add merge-q q-obj))
                                  (recur (.add trie pos)))

                                (let [pos (trie/write-live-trie meta-wtr trie (vw/rel-wtr->rdr data-wtr))]
                                  (.clear data-root)
                                  (.clear data-wtr)
                                  pos))))))]

          (trie/postwalk-merge-plan tries merge-nodes!)
          (.end meta-wtr))))))

(defn exec-compaction-job! [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                            {:keys [table-name trie-keys out-trie-key]}]
  (try
    (log/infof "compacting '%s' '%s' -> '%s'..." table-name trie-keys out-trie-key)
    (util/with-open [meta-files (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-name trie-keys nil)
                     data-out-bb (WritableByteBufferChannel/open)
                     meta-out-bb (WritableByteBufferChannel/open)]
      (doseq [trie-key trie-keys]
        (.add meta-files (trie/open-meta-file buffer-pool (trie/->table-meta-file-name table-name trie-key))))

      (merge-tries! allocator
                    (mapv :trie meta-files)
                    data-rels
                    (.getChannel data-out-bb) (.getChannel meta-out-bb))

      (log/debugf "uploading '%s' '%s'..." table-name out-trie-key)

      @(.putObject buffer-pool (trie/->table-data-file-name table-name out-trie-key)
                   (.getAsByteBuffer data-out-bb))
      @(.putObject buffer-pool (trie/->table-meta-file-name table-name out-trie-key)
                   (.getAsByteBuffer meta-out-bb)))

    (log/infof "compacted '%s' -> '%s'." table-name out-trie-key)

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn compaction-jobs [table-name meta-file-names]
  (for [[level parsed-trie-keys] (->> (trie/current-trie-files meta-file-names)
                                      (map trie/parse-trie-file-name)
                                      (group-by :level))
        job (partition 4 parsed-trie-keys)]
    {:table-name table-name
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
          (let [jobs (for [table-name (->> (.listObjects buffer-pool "tables")
                                           ;; TODO should obj-store listObjects only return keys from the current level?
                                           (into #{} (keep #(second (re-find #"^tables/([^/]+)" %)))))
                           job (compaction-jobs table-name (trie/list-meta-files buffer-pool table-name))]
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
