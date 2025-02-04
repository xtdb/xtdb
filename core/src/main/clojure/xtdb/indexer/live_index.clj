(ns xtdb.indexer.live-index
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.compactor :as c]
            [xtdb.metadata :as meta]
            [xtdb.metrics :as metrics]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
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
           (xtdb.api.log Log Log$Message$TriesAdded)
           xtdb.BufferPool
           (xtdb.indexer LiveIndex$Tx LiveIndex$Watermark LiveTable$Tx LiveTable$Watermark Watermark)
           xtdb.metadata.IMetadataManager
           (xtdb.trie MemoryHashTrie TrieCatalog)
           (xtdb.util RefCounter RowCounter)
           (xtdb.vector IRelationWriter IVectorWriter)))

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
    (reify LiveTable$Watermark
      (columnField [_ col-name]
        (get fields col-name (types/->field col-name #xt.arrow/type :null true)))

      (columnFields [_] fields)
      (liveRelation [_] wm-live-rel)
      (liveTrie [_] wm-live-trie)

      AutoCloseable
      (close [_] (util/close wm-live-rel)))))

(deftype LiveTable [^BufferAllocator allocator, ^BufferPool buffer-pool, ^RowCounter row-counter, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^MemoryHashTrie live-trie
                    ^IVectorWriter iid-wtr, ^IVectorWriter system-from-wtr, ^IVectorWriter valid-from-wtr, ^IVectorWriter valid-to-wtr
                    ^IVectorWriter put-wtr, ^IVectorWriter delete-wtr, ^IVectorWriter erase-wtr]
  xtdb.indexer.LiveTable
  (startTx [this-table tx-key new-live-table?]
    (let [!transient-trie (atom live-trie)
          system-from-µs (time/instant->micros (.getSystemTime tx-key))]
      (reify LiveTable$Tx
        (getDocWriter [_] put-wtr)

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

  (finishBlock [_ first-row next-row]
    (.syncRowCount live-rel)
    (let [row-count (.getPosition (.writerPosition live-rel))]
      (when (pos? row-count)
        (let [trie-key (trie/->log-l0-l1-trie-key 0 first-row next-row row-count)]
          (with-open [data-rel (.openAsRelation live-rel)]
            (let [data-file-size (trie/write-live-trie! allocator buffer-pool table-name trie-key
                                                        live-trie data-rel)]
              (MapEntry/create table-name
                               {:fields (live-rel->fields live-rel)
                                :trie-key trie-key
                                :data-file-size data-file-size
                                :row-count row-count})))))))

  (openWatermark [this] (live-table-wm live-rel (.live-trie this)))

  TestLiveTable
  (live-trie [_] live-trie)
  (live-rel [_] live-rel)

  AutoCloseable
  (close [_]
    (util/close live-rel)))

(defn ->live-table
  (^xtdb.indexer.LiveTable [allocator buffer-pool row-counter table-name]
   (->live-table allocator buffer-pool row-counter table-name {}))

  (^xtdb.indexer.LiveTable [allocator buffer-pool row-counter table-name
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

    (doseq [[table-name ^LiveTable live-table] tables]
      (.put wms table-name (.openWatermark live-table)))

    (reify LiveIndex$Watermark
      (getAllColumnFields [_] (update-vals wms #(.columnFields ^LiveTable$Watermark %)))

      (liveTable [_ table-name] (.get wms table-name))

      AutoCloseable
      (close [_] (util/close wms)))))

(defn ->schema [^LiveIndex$Watermark live-index-wm ^IMetadataManager metadata-mgr]
  (merge-with set/union
              (update-vals (.allColumnFields metadata-mgr)
                           (comp set keys))
              (update-vals (some-> live-index-wm
                                   (.getAllColumnFields))
                           (comp set keys))))


(deftype LiveIndex [^BufferAllocator allocator, ^BufferPool buffer-pool, ^IMetadataManager metadata-mgr
                    ^Log log, ^TrieCatalog trie-catalog, compactor
                    ^:volatile-mutable ^TransactionKey latest-completed-tx
                    ^:volatile-mutable ^TransactionKey latest-completed-block-tx
                    ^Map tables,

                    ^:volatile-mutable ^Watermark shared-wm
                    ^StampedLock wm-lock
                    ^RefCounter wm-cnt,

                    ^RowCounter row-counter, ^long rows-per-block

                    ^long log-limit, ^long page-limit]
  xtdb.indexer.LiveIndex
  (getLatestCompletedTx [_] latest-completed-tx)
  (getLatestCompletedBlockTx [_] latest-completed-block-tx)

  (liveTable [_ table-name] (.get tables table-name))

  (startTx [this-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify LiveIndex$Tx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (let [live-table (.liveTable this-idx table-name)
                                      new-live-table? (nil? live-table)
                                      ^LiveTable live-table (or live-table
                                                                (->live-table allocator buffer-pool row-counter table-name
                                                                              {:->live-trie (partial trie/->live-trie log-limit page-limit)}))]

                                  (.startTx live-table tx-key new-live-table?))))))

        (commit [_]
          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (doseq [[table-name ^LiveTable$Tx live-table-tx] table-txs]
                (.put tables table-name (.commit live-table-tx)))

              (set! (.-latest-completed-tx this-idx) tx-key)

              (let [^Watermark old-wm (.shared-wm this-idx)
                    ^Watermark shared-wm (util/with-close-on-catch [live-index-wm (open-live-idx-wm tables)]
                                           (Watermark. tx-key live-index-wm (->schema live-index-wm metadata-mgr)))]
                (set! (.shared-wm this-idx) shared-wm)
                (some-> old-wm .close))

              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (when (>= (.getBlockRowCount row-counter) rows-per-block)
            (.finishBlock this-idx)))

        (abort [_]
          (doseq [^LiveTable$Tx live-table-tx (.values table-txs)]
            (.abort live-table-tx))

          (set! (.-latest-completed-tx this-idx) tx-key)

          (when (>= (.getBlockRowCount row-counter) rows-per-block)
            (.finishBlock this-idx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^LiveTable$Tx live-table-tx] table-txs]
              (.put wms table-name (.openWatermark live-table-tx)))

            (doseq [[table-name ^LiveTable live-table] tables]
              (.computeIfAbsent wms table-name (fn [_] (.openWatermark live-table))))

            (reify LiveIndex$Watermark
              (getAllColumnFields [_] (update-vals wms #(.columnFields ^LiveTable$Watermark %)))
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

  (finishBlock [this]
    (let [block-idx (.getBlockIdx row-counter)
          next-block-idx (+ block-idx (.getBlockRowCount row-counter))]

      (log/debugf "finishing block 'rf%s-nr%s'..." (util/->lex-hex-string block-idx) (util/->lex-hex-string next-block-idx))

      (with-open [scope (StructuredTaskScope$ShutdownOnFailure.)]
        (let [tasks (vec (for [^LiveTable table (.values tables)]
                           (.fork scope (fn []
                                          (try
                                            (.finishBlock table block-idx next-block-idx)
                                            (catch InterruptedException e
                                              (throw e))
                                            (catch Exception e
                                              (log/warn e "Error finishing block for table" table)
                                              (throw e)))))))]
          (.join scope)

          (let [table-metadata (-> tasks
                                   (->> (into {} (keep #(try
                                                          (.get ^StructuredTaskScope$Subtask %)
                                                          (catch Exception _
                                                            (throw (.exception ^StructuredTaskScope$Subtask %)))))))
                                   (util/rethrowing-cause))]
            (.finishBlock metadata-mgr block-idx
                          {:latest-completed-tx latest-completed-tx

                           ;; TODO: :next-chunk-idx until we have a breaking index change
                           :next-chunk-idx next-block-idx
                           :tables table-metadata})

            (let [added-tries (for [[table-name {:keys [trie-key data-file-size]}] table-metadata]
                                (cat/->added-trie table-name trie-key data-file-size))]
              (doseq [added-trie added-tries]
                (.addTrie trie-catalog added-trie))

              @(.appendMessage log (Log$Message$TriesAdded. added-tries))))))

      (.nextBlock row-counter)

      (set! (.-latest_completed_block_tx this) latest-completed-tx)

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
      (log/debugf "finished block 'rf%s-nr%s'." (util/->lex-hex-string block-idx) (util/->lex-hex-string next-block-idx))))

  (forceFlush [this record msg]
    (let [expected-last-block-tx-id (.getExpectedBlockTxId msg)
          latest-block-tx-id (some-> latest-completed-block-tx (.getTxId))]
      (when (= (or latest-block-tx-id -1) expected-last-block-tx-id)
        (.finishBlock this)))

    (set! (.latest-completed-tx this)
          (serde/->TxKey (.getLogOffset record)
                         (.getLogTimestamp record))))

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
   :log (ig/ref :xtdb/log)
   :trie-catalog (ig/ref :xtdb/trie-catalog)
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :config config})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator buffer-pool metadata-mgr log trie-catalog compactor ^IndexerConfig config metrics-registry]}]
  ;; TODO: :next-chunk-idx until we have a breaking index change
  (let [{next-block-idx :next-chunk-idx, :keys [latest-completed-tx], :or {next-block-idx 0}} (meta/latest-block-metadata metadata-mgr)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
      (metrics/add-allocator-gauge metrics-registry "live-index.allocator.allocated_memory" allocator)
      (let [tables (HashMap.)]
        (->LiveIndex allocator buffer-pool metadata-mgr log trie-catalog compactor
                     latest-completed-tx latest-completed-tx
                     tables

                     (Watermark. nil (open-live-idx-wm tables) (->schema nil metadata-mgr))
                     (StampedLock.)
                     (RefCounter.)

                     (RowCounter. next-block-idx) (.getRowsPerBlock config)

                     (.getLogLimit config) (.getPageLimit config))))))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))

(defn finish-block! [node]
  (.finishBlock ^xtdb.indexer.LiveIndex (util/component node :xtdb.indexer/live-index)))
