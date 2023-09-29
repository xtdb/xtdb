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
           [java.util LinkedList]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot
           xtdb.buffer_pool.IBufferPool
           xtdb.object_store.ObjectStore
           (xtdb.trie CompactorLeafPointer ILeafLoader LiveHashTrie)
           xtdb.util.WritableByteBufferChannel))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll []))

(defn- ->log-leaf-schema [leaves]
  (trie/log-leaf-schema (->> (for [^ILeafLoader leaf leaves]
                               (-> (.getSchema leaf)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   (.getChildren) ^Field last
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn merge-tries! [^BufferAllocator allocator, tries, leaf-loaders, leaf-out-ch, trie-out-ch]
  (let [log-leaf-schema (->log-leaf-schema leaf-loaders)]

    (util/with-open [trie-wtr (trie/open-trie-writer allocator log-leaf-schema
                                                     leaf-out-ch trie-out-ch)

                     leaf-root (VectorSchemaRoot/create log-leaf-schema allocator)]

      (let [leaf-wtr (vw/root->writer leaf-root)
            is-valid-ptr (ArrowBufPointer.)]
        (letfn [(merge-nodes! [path [mn-tag mn-arg]]
                  (case mn-tag
                    :branch (.writeBranch trie-wtr (int-array mn-arg))

                    :leaf (let [loaded-leaves (trie/load-leaves leaf-loaders mn-arg)
                                merge-q (trie/->merge-queue)]

                            (doseq [leaf-rdr loaded-leaves
                                    :when leaf-rdr
                                    :let [leaf-ptr (CompactorLeafPointer. leaf-rdr (.rowCopier leaf-wtr leaf-rdr))]]
                              (when (.isValid leaf-ptr is-valid-ptr path)
                                (.add merge-q leaf-ptr)))

                            (loop [trie (-> (doto (LiveHashTrie/builder (vr/vec->reader (.getVector leaf-root "xt$iid")))
                                              (.setRootPath path))
                                            (.build))]

                              (if-let [^CompactorLeafPointer leaf-ptr (.poll merge-q)]
                                (let [pos (.copyRow (.rowCopier leaf-ptr) (.getIndex leaf-ptr))]
                                  (.nextIndex leaf-ptr)
                                  (when (.isValid leaf-ptr is-valid-ptr path)
                                    (.add merge-q leaf-ptr))
                                  (recur (.add trie pos)))

                                (let [pos (trie/write-live-trie trie-wtr trie (vw/rel-wtr->rdr leaf-wtr))]
                                  (.clear leaf-root)
                                  (.clear leaf-wtr)
                                  pos))))))]

          (trie/postwalk-merge-tries tries merge-nodes!)
          (.end trie-wtr))))))

(defn exec-compaction-job! [^BufferAllocator allocator, ^ObjectStore obj-store, ^IBufferPool buffer-pool,
                            {:keys [table-name table-tries out-trie-key]}]
  (try
    (log/infof "compacting '%s' '%s' -> '%s'..." table-name (mapv :trie-key table-tries) out-trie-key)
    (util/with-open [arrow-tries (LinkedList.)
                     leaf-loaders (trie/open-leaves buffer-pool table-name table-tries nil)
                     leaf-out-bb (WritableByteBufferChannel/open)
                     trie-out-bb (WritableByteBufferChannel/open)]
      (doseq [table-trie table-tries]
        (.add arrow-tries (trie/open-arrow-trie-file buffer-pool table-trie)))

      (merge-tries! allocator
                    (mapv :trie arrow-tries)
                    leaf-loaders
                    (.getChannel leaf-out-bb) (.getChannel trie-out-bb))

      (log/debugf "uploading '%s' '%s'..." table-name out-trie-key)

      @(.putObject obj-store (trie/->table-leaf-obj-key table-name out-trie-key)
                   (.getAsByteBuffer leaf-out-bb))
      @(.putObject obj-store (trie/->table-trie-obj-key table-name out-trie-key)
                   (.getAsByteBuffer trie-out-bb)))

    (log/infof "compacted '%s' -> '%s'." table-name out-trie-key)

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn compaction-jobs [table-name table-tries]
  (for [[level table-tries] (->> (trie/current-table-tries table-tries)
                                 (group-by :level))
        job (partition 4 table-tries)]
    {:table-name table-name
     :table-tries job
     :out-trie-key (trie/->trie-key (inc level)
                                    (:row-from (first job))
                                    (:row-to (last job)))}))

(defmethod ig/prep-key :xtdb/compactor [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :obj-store (ig/ref :xtdb/object-store)
         :buffer-pool (ig/ref :xtdb.buffer-pool/buffer-pool)}
        opts))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [allocator ^ObjectStore obj-store buffer-pool]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (reify ICompactor
      (compactAll [_]
        (log/info "compact-all")
        (loop []
          (let [jobs (for [table-name (->> (.listObjects obj-store "tables")
                                           ;; TODO should obj-store listObjects only return keys from the current level?
                                           (into #{} (keep #(second (re-find #"^tables/([^/]+)" %)))))
                           job (compaction-jobs table-name (->> (trie/list-table-trie-files obj-store table-name)
                                                                (trie/current-table-tries)))]
                       job)
                jobs? (boolean (seq jobs))]

            (doseq [job jobs]
              (exec-compaction-job! allocator obj-store buffer-pool job))

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
