(ns xtdb.indexer.live-index
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.compactor :as c]
            [xtdb.metadata :as meta]
            [xtdb.metrics :as metrics]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent StructuredTaskScope$ShutdownOnFailure StructuredTaskScope$Subtask)
           (java.util.concurrent.locks StampedLock)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.api IndexerConfig TransactionKey)
           xtdb.IBufferPool
           xtdb.metadata.IMetadataManager
           (xtdb.trie MemoryHashTrie)
           (xtdb.util RefCounter RowCounter)
           (xtdb.vector IRelationWriter IVectorWriter)
           (xtdb.watermark ILiveIndexWatermark ILiveTableWatermark Watermark)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.watermark.ILiveTableWatermark openWatermark [])
  (^xtdb.vector.IVectorWriter docWriter [])
  (^void logPut [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo, writeDocFn])
  (^void logDelete [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo])
  (^void logErase [^java.nio.ByteBuffer iid])
  (^xtdb.indexer.live_index.ILiveTable commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [^xtdb.api.TransactionKey txKey
                                                  ^boolean newLiveTable])
  (^xtdb.watermark.ILiveTableWatermark openWatermark [])
  (^java.util.List #_<Map$Entry> finishChunk [^long firstRow ^long nextRow])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^xtdb.watermark.ILiveIndexWatermark openWatermark [])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.api.TransactionKey latestCompletedTx [])
  (^xtdb.api.TransactionKey latestCompletedChunkTx [])

  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [^xtdb.api.TransactionKey txKey])

  (^xtdb.watermark.Watermark openWatermark [])

  (^void close []))

(defprotocol FinishChunk
  (^void finish-chunk! [_])
  (^void force-flush! [_ tx-key expected-last-chunk-tx-id]))

(defprotocol TestLiveTable
  (^MemoryHashTrie live-trie [test-live-table])
  (^xtdb.vector.IRelationWriter live-rel [test-live-table]))

(defn- live-rel->fields [^IRelationWriter live-rel]
  (let [live-rel-field (-> (.colWriter live-rel "op")
                           (.legWriter "put")
                           .getField)]
    (assert (= #xt.arrow/type :struct (.getType live-rel-field)))
    (into {} (map (comp (juxt #(.getName ^Field %) identity))) (.getChildren live-rel-field))))

(defn- open-wm-live-rel ^xtdb.vector.RelationReader [^IRelationWriter rel]
  (let [out-cols (ArrayList.)]
    (try
      (doseq [^IVectorWriter w (vals rel)]
        (.syncValueCount w)
        (.add out-cols (vr/vec->reader (util/slice-vec (.getVector w)))))

      (vr/rel-reader out-cols)

      (catch Throwable t
        (util/close out-cols)
        (throw t)))))

(defn live-table-wm [^IRelationWriter live-rel, trie]
  (let [fields (live-rel->fields live-rel)
        wm-live-rel (open-wm-live-rel live-rel)
        wm-live-trie (-> ^MemoryHashTrie trie
                         (.withIidReader (.readerForName wm-live-rel "_iid")))]
    (reify ILiveTableWatermark
      (columnField [_ col-name]
        (get fields col-name (types/->field col-name #xt.arrow/type :null true)))

      (columnFields [_] fields)
      (liveRelation [_] wm-live-rel)
      (liveTrie [_] wm-live-trie)

      AutoCloseable
      (close [_] (util/close wm-live-rel)))))

(deftype LiveTable [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^RowCounter row-counter, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^MemoryHashTrie live-trie
                    ^IVectorWriter iid-wtr, ^IVectorWriter system-from-wtr, ^IVectorWriter valid-from-wtr, ^IVectorWriter valid-to-wtr
                    ^IVectorWriter put-wtr, ^IVectorWriter delete-wtr, ^IVectorWriter erase-wtr]
  ILiveTable
  (startTx [this-table tx-key new-live-table?]
    (let [!transient-trie (atom live-trie)
          system-from-µs (time/instant->micros (.getSystemTime tx-key))]
      (reify ILiveTableTx
        (docWriter [_] put-wtr)

        (logPut [_ iid valid-from valid-to write-doc!]
          (.startRow live-rel)

          (.writeBytes iid-wtr iid)

          (.writeLong system-from-wtr system-from-µs)
          (.writeLong valid-from-wtr valid-from)
          (.writeLong valid-to-wtr valid-to)

          (write-doc!)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^MemoryHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (logDelete [_ iid valid-from valid-to]
          (.writeBytes iid-wtr iid)

          (.writeLong system-from-wtr system-from-µs)
          (.writeLong valid-from-wtr valid-from)
          (.writeLong valid-to-wtr valid-to)

          (.writeNull delete-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^MemoryHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (logErase [_ iid]
          (.writeBytes iid-wtr iid)

          (.writeLong system-from-wtr system-from-µs)
          (.writeLong valid-from-wtr Long/MIN_VALUE)
          (.writeLong valid-to-wtr Long/MAX_VALUE)

          (.writeNull erase-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^MemoryHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (openWatermark [_] (live-table-wm live-rel @!transient-trie))

        (commit [_]
          (set! (.-live-trie this-table) @!transient-trie)
          this-table)

        (abort [_]
          (when new-live-table?
            (util/close this-table)))

        AutoCloseable
        (close [_]))))

  (finishChunk [_ first-row next-row]
    (.syncRowCount live-rel)
    (let [row-count (.getPosition (.writerPosition live-rel))]
      (when (pos? row-count)
        (with-open [data-rel (.openAsRelation live-rel)]
          (trie/write-live-trie! allocator buffer-pool
                                 (util/table-name->table-path table-name)
                                 (trie/->log-l0-l1-trie-key 0 first-row next-row row-count)
                                 live-trie data-rel)

          (MapEntry/create table-name
                           {:fields (live-rel->fields live-rel)
                            :row-count row-count})))))

  (openWatermark [this] (live-table-wm live-rel (.live-trie this)))

  TestLiveTable
  (live-trie [_] live-trie)
  (live-rel [_] live-rel)

  AutoCloseable
  (close [_]
    (util/close live-rel)))

(defn ->live-table
  (^xtdb.indexer.live_index.ILiveTable [allocator buffer-pool row-counter table-name]
   (->live-table allocator buffer-pool row-counter table-name {}))

  (^xtdb.indexer.live_index.ILiveTable [allocator buffer-pool row-counter table-name
                                        {:keys [->live-trie]
                                         :or {->live-trie (fn [iid-rdr]
                                                            (MemoryHashTrie/emptyTrie iid-rdr))}}]
   (util/with-close-on-catch [rel (trie/open-log-data-wtr allocator)]
     (let [iid-wtr (.colWriter rel "_iid")
           op-wtr (.colWriter rel "op")]
       (->LiveTable allocator buffer-pool row-counter table-name rel
                    (->live-trie (vw/vec-wtr->rdr iid-wtr))
                    iid-wtr (.colWriter rel "_system_from")
                    (.colWriter rel "_valid_from") (.colWriter rel "_valid_to")
                    (.legWriter op-wtr "put") (.legWriter op-wtr "delete") (.legWriter op-wtr "erase"))))))


(defn open-live-idx-wm [^Map tables]
  (util/with-close-on-catch [wms (HashMap.)]

    (doseq [[table-name ^ILiveTable live-table] tables]
      (.put wms table-name (.openWatermark live-table)))

    (reify ILiveIndexWatermark
      (allColumnFields [_] (update-vals wms #(.columnFields ^ILiveTableWatermark %)))

      (liveTable [_ table-name] (.get wms table-name))

      AutoCloseable
      (close [_] (util/close wms)))))

(defn ->schema [^ILiveIndexWatermark live-index-wm ^IMetadataManager metadata-mgr]
  (merge-with set/union
              (update-vals (.allColumnFields metadata-mgr)
                           (comp set keys))
              (update-vals (some-> live-index-wm
                                   (.allColumnFields))
                           (comp set keys))))


(deftype LiveIndex [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr, compactor
                    ^:volatile-mutable ^TransactionKey latest-completed-tx
                    ^:volatile-mutable ^TransactionKey latest-completed-chunk-tx
                    ^Map tables,

                    ^:volatile-mutable ^Watermark shared-wm
                    ^StampedLock wm-lock
                    ^RefCounter wm-cnt,

                    ^RowCounter row-counter, ^long rows-per-chunk

                    ^long log-limit, ^long page-limit]
  ILiveIndex
  (latestCompletedTx [_] latest-completed-tx)
  (latestCompletedChunkTx [_] latest-completed-chunk-tx)

  (liveTable [_ table-name] (.get tables table-name))

  (startTx [this-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (let [live-table (.liveTable this-idx table-name)
                                      new-live-table? (nil? live-table)
                                      ^ILiveTable live-table (or live-table
                                                                 (->live-table allocator buffer-pool row-counter table-name
                                                                               {:->live-trie (partial trie/->live-trie log-limit page-limit)}))]

                                  (.startTx live-table tx-key new-live-table?))))))

        (commit [_]
          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (doseq [[table-name ^ILiveTableTx live-table-tx] table-txs]
                (.put tables table-name (.commit live-table-tx)))

              (set! (.-latest-completed-tx this-idx) tx-key)

              (let [^Watermark old-wm (.shared-wm this-idx)
                    ^Watermark shared-wm (util/with-close-on-catch [live-index-wm (open-live-idx-wm tables)]
                                           (Watermark.
                                            (.latestCompletedTx this-idx)
                                            live-index-wm
                                            (->schema live-index-wm metadata-mgr)))]
                (set! (.shared-wm this-idx) shared-wm)
                (some-> old-wm .close))

              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (when (>= (.getChunkRowCount row-counter) rows-per-chunk)
            (finish-chunk! this-idx)))

        (abort [_]
          (doseq [^ILiveTableTx live-table-tx (.values table-txs)]
            (.abort live-table-tx))

          (set! (.-latest-completed-tx this-idx) tx-key)

          (when (>= (.getChunkRowCount row-counter) rows-per-chunk)
            (finish-chunk! this-idx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^ILiveTableTx live-table-tx] table-txs]
              (.put wms table-name (.openWatermark live-table-tx)))

            (doseq [[table-name ^ILiveTable live-table] tables]
              (.computeIfAbsent wms table-name (fn [_] (.openWatermark live-table))))

            (reify ILiveIndexWatermark
              (allColumnFields [_] (update-vals wms #(.columnFields ^ILiveTableWatermark %)))
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_] (util/close wms)))))

        AutoCloseable
        (close [_]))))

  (openWatermark [this]
    (let [wm-read-stamp (.readLock wm-lock)]
      (try
        (doto ^Watermark (.-shared-wm this) .retain)
        (finally
          (.unlock wm-lock wm-read-stamp)))))

  FinishChunk
  (finish-chunk! [this]
    (let [chunk-idx (.getChunkIdx row-counter)
          next-chunk-idx (+ chunk-idx (.getChunkRowCount row-counter))]

      (log/debugf "finishing chunk 'rf%s-nr%s'..." (util/->lex-hex-string chunk-idx) (util/->lex-hex-string next-chunk-idx))

      (with-open [scope (StructuredTaskScope$ShutdownOnFailure.)]
        (let [tasks (vec (for [^ILiveTable table (.values tables)]
                           (.fork scope (fn []
                                          (.finishChunk table chunk-idx next-chunk-idx)))))]
          (.join scope)

          (let [table-metadata (-> (into {} (keep #(try
                                                     (.get ^StructuredTaskScope$Subtask %)
                                                     (catch Exception _
                                                       (throw (.exception ^StructuredTaskScope$Subtask %)))))
                                         tasks)
                                   (util/rethrowing-cause))]
            (.finishChunk metadata-mgr chunk-idx
                          {:latest-completed-tx latest-completed-tx
                           :next-chunk-idx next-chunk-idx
                           :tables table-metadata})
            (future
              (try
                (.cleanUp metadata-mgr)
                (catch Exception e
                  (log/error e "Error while cleaning up the metadata manager.")))))))

      (.nextChunk row-counter)

      (set! (.-latest_completed_chunk_tx this) latest-completed-tx)

      (let [wm-lock-stamp (.writeLock wm-lock)]
        (try
          (let [^Watermark shared-wm (.shared-wm this)]
            (.close shared-wm))

          (util/close tables)
          (.clear tables)
          (set! (.shared-wm this)
                (util/with-close-on-catch [live-index-wm (open-live-idx-wm tables)]
                  (Watermark.
                   latest-completed-tx
                   live-index-wm
                   (->schema live-index-wm metadata-mgr))))
          (finally
            (.unlock wm-lock wm-lock-stamp))))

      (c/signal-block! compactor)
      (log/debugf "finished chunk 'rf%s-nr%s'." (util/->lex-hex-string chunk-idx) (util/->lex-hex-string next-chunk-idx))))

  (force-flush! [this tx-key expected-last-chunk-tx-id]
    (let [latest-chunk-tx-id (some-> (.latestCompletedChunkTx this) (.getTxId))]
      (when (= (or latest-chunk-tx-id -1) expected-last-chunk-tx-id)
        (finish-chunk! this)))

    (set! (.latest-completed-tx this) tx-key))

  AutoCloseable
  (close [_]
    (some-> shared-wm .close)
    (util/close tables)
    (if-not (.tryClose wm-cnt (Duration/ofMinutes 1))
      (log/warn "Failed to shut down live-index after 60s due to outstanding watermarks.")
      (util/close allocator))))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref ::meta/metadata-manager)
   :compactor (ig/ref :xtdb/compactor)
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :config config})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator buffer-pool metadata-mgr compactor ^IndexerConfig config metrics-registry]}]
  (let [{:keys [latest-completed-tx next-chunk-idx], :or {next-chunk-idx 0}} (meta/latest-chunk-metadata metadata-mgr)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
      (metrics/add-allocator-gauge metrics-registry "live-index.allocator.allocated_memory" allocator)
      (let [tables (HashMap.)]
        (->LiveIndex allocator buffer-pool metadata-mgr compactor
                     latest-completed-tx latest-completed-tx
                     tables

                     (Watermark. nil (open-live-idx-wm tables) (->schema nil metadata-mgr))
                     (StampedLock.)
                     (RefCounter.)

                     (RowCounter. next-chunk-idx) (.getRowsPerChunk config)

                     (.getLogLimit config) (.getPageLimit config))))))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))
