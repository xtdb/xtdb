(ns xtdb.indexer.live-index
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent CompletableFuture CompletionException)
           (java.util.concurrent.locks StampedLock)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.api IndexerConfig TransactionKey)
           xtdb.IBufferPool
           xtdb.metadata.IMetadataManager
           (xtdb.trie LiveHashTrie)
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
  (^xtdb.watermark.ILiveTableWatermark openWatermark [^boolean retain])
  (^java.util.concurrent.CompletableFuture #_<List<Map$Entry>> finishChunk [^long nextRow])
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

  (^xtdb.watermark.Watermark openWatermark [^xtdb.api.TransactionKey txKey])

  (^void close []))

(defprotocol FinishChunk
  (^void finish-chunk! [_])
  (^void force-flush! [_ tx-key expected-last-chunk-tx-id]))

(defprotocol TestLiveTable
  (^xtdb.trie.LiveHashTrie live-trie [test-live-table])
  (^xtdb.vector.IRelationWriter live-rel [test-live-table]))

(defn- live-rel->fields [^IRelationWriter live-rel]
  (let [live-rel-field (-> (.colWriter live-rel "op")
                           (.legWriter :put)
                           .getField)]
    (assert (= #xt.arrow/type :struct (.getType live-rel-field)))
    (into {} (map (comp (juxt #(.getName ^Field %) identity))) (.getChildren live-rel-field))))

(defn- open-wm-live-rel ^xtdb.vector.RelationReader [^IRelationWriter rel, retain?]
  (let [out-cols (ArrayList.)]
    (try
      (doseq [^IVectorWriter w (vals rel)]
        (.syncValueCount w)
        (.add out-cols (vr/vec->reader (cond-> (.getVector w)
                                         retain? (util/slice-vec)))))

      (vr/rel-reader out-cols)

      (catch Throwable t
        (when retain? (util/close out-cols))
        (throw t)))))

(deftype LiveTable [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^RowCounter row-counter, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^LiveHashTrie live-trie
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

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (logDelete [_ iid valid-from valid-to]
          (.writeBytes iid-wtr iid)

          (.writeLong system-from-wtr system-from-µs)
          (.writeLong valid-from-wtr valid-from)
          (.writeLong valid-to-wtr valid-to)

          (.writeNull delete-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (logErase [_ iid]
          (.writeBytes iid-wtr iid)

          (.writeLong system-from-wtr system-from-µs)
          (.writeLong valid-from-wtr Long/MIN_VALUE)
          (.writeLong valid-to-wtr Long/MAX_VALUE)

          (.writeNull erase-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel)))))
          (.addRows row-counter 1))

        (openWatermark [_]
          (let [fields (live-rel->fields live-rel)
                wm-live-rel (open-wm-live-rel live-rel false)
                wm-live-trie @!transient-trie]

            (reify ILiveTableWatermark
              (columnFields [_] fields)
              (liveRelation [_] wm-live-rel)
              (liveTrie [_] wm-live-trie)

              AutoCloseable
              (close [_]))))

        (commit [_]
          (set! (.-live-trie this-table) @!transient-trie)
          this-table)

        (abort [_]
          (when new-live-table?
            (util/close this-table)))

        AutoCloseable
        (close [_]))))

  (finishChunk [_ next-chunk-idx]
    (let [live-rel-rdr (vw/rel-wtr->rdr live-rel)
          row-count (.rowCount live-rel-rdr)]
      (when (pos? row-count)
        (let [!fut (CompletableFuture/runAsync
                    (fn []
                      (trie/write-live-trie! allocator buffer-pool
                                             (util/table-name->table-path table-name)
                                             (trie/->log-l0-l1-trie-key 0 next-chunk-idx row-count)
                                             live-trie live-rel-rdr)))
              table-metadata (MapEntry/create table-name
                                              {:fields (live-rel->fields live-rel)
                                               :row-count (.rowCount live-rel-rdr)})]
          (-> !fut
              (util/then-apply (fn [_] table-metadata)))))))

  (openWatermark [this retain?]
    (let [fields (live-rel->fields live-rel)
          wm-live-rel (open-wm-live-rel live-rel retain?)
          wm-live-trie (.withIidReader ^LiveHashTrie (.live-trie this) (.readerForName wm-live-rel "xt$iid"))]
      (reify ILiveTableWatermark
        (columnFields [_] fields)
        (liveRelation [_] wm-live-rel)
        (liveTrie [_] wm-live-trie)

        AutoCloseable
        (close [_]
          (when retain? (util/close wm-live-rel))))))

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
                                                            (LiveHashTrie/emptyTrie iid-rdr))}}]
   (util/with-close-on-catch [rel (trie/open-log-data-wtr allocator)]
     (let [iid-wtr (.colWriter rel "xt$iid")
           op-wtr (.colWriter rel "op")]
       (->LiveTable allocator buffer-pool row-counter table-name rel
                    (->live-trie (vw/vec-wtr->rdr iid-wtr))
                    iid-wtr (.colWriter rel "xt$system_from")
                    (.colWriter rel "xt$valid_from") (.colWriter rel "xt$valid_to")
                    (.legWriter op-wtr :put) (.legWriter op-wtr :delete) (.legWriter op-wtr :erase))))))

(defn ->live-trie [log-limit page-limit iid-rdr]
  (-> (doto (LiveHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(deftype LiveIndex [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr
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
                                                                               {:->live-trie (partial ->live-trie log-limit page-limit)}))]

                                  (.startTx live-table tx-key new-live-table?))))))

        (commit [_]
          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (doseq [[table-name ^ILiveTableTx live-table-tx] table-txs]
                (.put tables table-name (.commit live-table-tx)))

              (set! (.-latest-completed-tx this-idx) tx-key)
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
              (.computeIfAbsent wms table-name
                                (util/->jfn (fn [_] (.openWatermark live-table false)))))

            (reify ILiveIndexWatermark
              (allColumnFields [_] (update-vals wms #(.columnFields ^ILiveTableWatermark %)))
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_]
                (util/close wms)))))

        AutoCloseable
        (close [_]))))

  (openWatermark [this tx-key]
    (letfn [(maybe-existing-wm []
              (when-let [^Watermark wm (.shared-wm this)]
                (let [wm-tx-key (.txBasis wm)]
                  (when (or (nil? tx-key)
                            (and wm-tx-key
                                 (<= (.getTxId tx-key) (.getTxId wm-tx-key))))
                    (doto wm .retain)))))
            (open-live-idx-wm []
              (.acquire wm-cnt)
              (try
                (util/with-close-on-catch [wms (HashMap.)]

                  (doseq [[table-name ^ILiveTable live-table] tables]
                    (.put wms table-name (.openWatermark live-table true)))

                  (reify ILiveIndexWatermark
                    (allColumnFields [_] (update-vals wms #(.columnFields ^ILiveTableWatermark %)))

                    (liveTable [_ table-name] (.get wms table-name))

                    AutoCloseable
                    (close [_]
                      (util/close wms)
                      (.release wm-cnt))))
                (catch Throwable t
                  (.release wm-cnt)
                  (throw t))))]

      (or (let [wm-lock-stamp (.readLock wm-lock)]
            (try
              (maybe-existing-wm)
              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (or (maybe-existing-wm)
                  (let [^Watermark old-wm (.shared-wm this)]
                    (try
                      (let [^Watermark shared-wm (Watermark. (.latestCompletedTx this) (open-live-idx-wm))]
                        (set! (.shared-wm this) shared-wm)
                        (doto shared-wm .retain))
                      (finally
                        (some-> old-wm .close)))))

              (finally
                (.unlock wm-lock wm-lock-stamp)))))))

  FinishChunk
  (finish-chunk! [this]
    (let [chunk-idx (.getChunkIdx row-counter)
          next-chunk-idx (+ chunk-idx (.getChunkRowCount row-counter))

          futs (->> (for [^ILiveTable table (.values tables)]
                      (.finishChunk table next-chunk-idx))

                    (remove nil?)
                    (into-array CompletableFuture))]

      ;; TODO currently non interruptible, meaning this waits for the tries to be written
      (try
        (.join (CompletableFuture/allOf futs))
        (catch CompletionException e
          (throw (.getCause e))))

      (let [table-metadata (-> (into {} (keep deref) futs)
                               (util/rethrowing-cause))]
        (.finishChunk metadata-mgr chunk-idx
                      {:latest-completed-tx latest-completed-tx
                       :next-chunk-idx next-chunk-idx
                       :tables table-metadata}))

      (.nextChunk row-counter)

      (set! (.-latest_completed_chunk_tx this) latest-completed-tx)

      (let [wm-lock-stamp (.writeLock wm-lock)]
        (try
          (when-let [^Watermark shared-wm (.shared-wm this)]
            (set! (.shared-wm this) nil)
            (.close shared-wm))

          (finally
            (.unlock wm-lock wm-lock-stamp))))

      (util/close tables)
      (.clear tables)

      (log/debugf "finished chunk 'rf%s-nr%s'." (util/->lex-hex-string chunk-idx) (util/->lex-hex-string next-chunk-idx))))

  (force-flush! [this tx-key expected-last-chunk-tx-id]
    (let [latest-chunk-tx-id (some-> (.latestCompletedChunkTx this) (.getTxId))]
      (when (= (or latest-chunk-tx-id -1) expected-last-chunk-tx-id)
        (finish-chunk! this)))

    (set! (.latest-completed-tx this) tx-key))

  AutoCloseable
  (close [_]
    (util/close tables)
    (some-> shared-wm .close)
    (if-not (.tryClose wm-cnt (Duration/ofMinutes 1))
      (log/warn "Failed to shut down live-index after 60s due to outstanding watermarks.")
      (util/close allocator))))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref ::meta/metadata-manager)
   :config config})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator buffer-pool metadata-mgr ^IndexerConfig config]}]
  (let [{:keys [latest-completed-tx next-chunk-idx], :or {next-chunk-idx 0}} (meta/latest-chunk-metadata metadata-mgr)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
      (->LiveIndex allocator buffer-pool metadata-mgr
                   latest-completed-tx latest-completed-tx
                   (HashMap.)

                   nil ;; watermark
                   (StampedLock.)
                   (RefCounter.)

                   (RowCounter. next-chunk-idx) (.getRowsPerChunk config)

                   (.getLogLimit config) (.getPageLimit config)))))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))
