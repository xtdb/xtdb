(ns xtdb.indexer.live-index
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.object-store]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           xtdb.IBufferPool
           (xtdb.object_store ObjectStore)
           (xtdb.trie LiveHashTrie)
           (xtdb.util RefCounter)
           (xtdb.vector IRelationWriter IVectorWriter)
           (xtdb.watermark ILiveIndexWatermark ILiveTableWatermark)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.watermark.ILiveTableWatermark openWatermark [])
  (^xtdb.vector.IVectorWriter docWriter [])
  (^void logPut [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo, writeDocFn])
  (^void logDelete [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo])
  (^void logEvict [^java.nio.ByteBuffer iid])
  (^xtdb.indexer.live_index.ILiveTable commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [^xtdb.api.protocols.TransactionInstant txKey
                                                  ^boolean newLiveTable])
  (^xtdb.watermark.ILiveTableWatermark openWatermark [^boolean retain])
  (^java.util.concurrent.CompletableFuture #_<List<Map$Entry>> finishChunk [^String trieKey])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^xtdb.watermark.ILiveIndexWatermark openWatermark [])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^xtdb.watermark.ILiveIndexWatermark openWatermark [])
  (^java.util.Map finishChunk [^long chunkIdx, ^long nextChunkIdx])
  (^void nextChunk [])
  (^void close []))

(defprotocol TestLiveTable
  (^xtdb.trie.LiveHashTrie live-trie [test-live-table])
  (^xtdb.vector.IRelationWriter live-rel [test-live-table]))

(defn- live-rel->col-types [^IRelationWriter live-rel]
  (let [col-type (-> (.colWriter live-rel "op")
                     (.legWriter :put)
                     (.structKeyWriter "xt$doc")
                     .getField
                     types/field->col-type
                     types/without-null)]
    (assert (= :struct (types/col-type-head col-type)))
    (into {} (map (juxt (comp str key) val)) (second col-type))))

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

(deftype LiveTable [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^LiveHashTrie live-trie
                    ^IVectorWriter iid-wtr, ^IVectorWriter system-from-wtr
                    ^IVectorWriter put-wtr, ^IVectorWriter put-valid-from-wtr, ^IVectorWriter put-valid-to-wtr, ^IVectorWriter put-doc-wtr
                    ^IVectorWriter delete-wtr, ^IVectorWriter delete-valid-from-wtr, ^IVectorWriter delete-valid-to-wtr
                    ^IVectorWriter evict-wtr]
  ILiveTable
  (startTx [this-table tx-key new-live-table?]
    (let [!transient-trie (atom live-trie)
          system-from-µs (util/instant->micros (.system-time tx-key))]
      (reify ILiveTableTx
        (docWriter [_] put-doc-wtr)

        (logPut [_ iid valid-from valid-to write-doc!]
          (.startRow live-rel)

          (.writeBytes iid-wtr iid)
          (.writeLong system-from-wtr system-from-µs)

          (.startStruct put-wtr)
          (.writeLong put-valid-from-wtr valid-from)
          (.writeLong put-valid-to-wtr valid-to)
          (write-doc!)
          (.endStruct put-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (logDelete [_ iid valid-from valid-to]
          (.writeBytes iid-wtr iid)
          (.writeLong system-from-wtr system-from-µs)

          (.startStruct delete-wtr)
          (.writeLong delete-valid-from-wtr valid-from)
          (.writeLong delete-valid-to-wtr valid-to)
          (.endStruct delete-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (logEvict [_ iid]
          (.writeBytes iid-wtr iid)
          (.writeLong system-from-wtr system-from-µs)

          (.writeNull evict-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (openWatermark [_]
          (let [col-types (live-rel->col-types live-rel)
                wm-live-rel (open-wm-live-rel live-rel false)
                wm-live-trie @!transient-trie]

            (reify ILiveTableWatermark
              (columnTypes [_] col-types)
              (liveRelation [_] wm-live-rel)
              (liveTrie [_] wm-live-trie)

              AutoCloseable
              (close [_]))))

        (commit [_]
          (set! (.-live-trie this-table) @!transient-trie)
          this-table)

        (abort [_]
          (when new-live-table?
            (util/close this-table))))))

  (finishChunk [_ trie-key]
    (let [live-rel-rdr (vw/rel-wtr->rdr live-rel)]
      (when (pos? (.rowCount live-rel-rdr))
        (let [bufs (trie/live-trie->bufs allocator live-trie live-rel-rdr)
              !fut (trie/write-trie-bufs! buffer-pool table-name trie-key bufs)
              table-metadata (MapEntry/create table-name
                                              {:col-types (live-rel->col-types live-rel)
                                               :row-count (.rowCount live-rel-rdr)})]
          (-> !fut
              (util/then-apply (fn [_] table-metadata)))))))

  (openWatermark [this retain?]
    (let [col-types (live-rel->col-types live-rel)
          wm-live-rel (open-wm-live-rel live-rel retain?)
          wm-live-trie (.withIidReader ^LiveHashTrie (.live-trie this) (.readerForName wm-live-rel "xt$iid"))]
      (reify ILiveTableWatermark
        (columnTypes [_] col-types)
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
  (^xtdb.indexer.live_index.ILiveTable [allocator buffer-pool table-name] (->live-table allocator buffer-pool table-name {}))

  (^xtdb.indexer.live_index.ILiveTable [allocator buffer-pool table-name
                                        {:keys [->live-trie]
                                         :or {->live-trie (fn [iid-rdr]
                                                            (LiveHashTrie/emptyTrie iid-rdr))}}]
   (util/with-close-on-catch [rel (trie/open-log-data-root allocator)]
     (let [iid-wtr (.colWriter rel "xt$iid")
           op-wtr (.colWriter rel "op")
           put-wtr (.legWriter op-wtr :put)
           delete-wtr (.legWriter op-wtr :delete)]
       (->LiveTable allocator buffer-pool table-name rel
                    (->live-trie (vw/vec-wtr->rdr iid-wtr))
                    iid-wtr (.colWriter rel "xt$system_from")
                    put-wtr (.structKeyWriter put-wtr "xt$valid_from") (.structKeyWriter put-wtr "xt$valid_to")
                    (.structKeyWriter put-wtr "xt$doc") delete-wtr (.structKeyWriter delete-wtr "xt$valid_from")
                    (.structKeyWriter delete-wtr "xt$valid_to")
                    (.legWriter op-wtr :evict))))))

(defn ->live-trie [log-limit page-limit iid-rdr]
  (-> (doto (LiveHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defrecord LiveIndex [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                      ^Map tables, ^RefCounter wm-cnt, ^long log-limit, ^long page-limit]
  ILiveIndex
  (liveTable [_ table-name] (.get tables table-name))

  (startTx [this-table tx-key]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (let [live-table (.liveTable this-table table-name)
                                      new-live-table? (nil? live-table)
                                      ^ILiveTable live-table (or live-table
                                                                 (->live-table allocator buffer-pool table-name
                                                                               {:->live-trie (partial ->live-trie log-limit page-limit)}))]

                                  (.startTx live-table tx-key new-live-table?))))))

        (commit [_]
          (doseq [[table-name ^ILiveTableTx live-table-tx] table-txs]
            (.put tables table-name (.commit live-table-tx))))

        (abort [_]
          (doseq [^ILiveTableTx live-table-tx (.values table-txs)]
            (.abort live-table-tx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^ILiveTableTx live-table-tx] table-txs]
              (.put wms table-name (.openWatermark live-table-tx)))

            (doseq [[table-name ^ILiveTable live-table] tables]
              (.computeIfAbsent wms table-name
                                (util/->jfn (fn [_] (.openWatermark live-table false)))))

            (reify ILiveIndexWatermark
              (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_]
                (util/close wms)))))

        AutoCloseable
        (close [_]))))

  (openWatermark [_]
    (.acquire wm-cnt)
    (try
      (util/with-close-on-catch [wms (HashMap.)]

        (doseq [[table-name ^ILiveTable live-table] tables]
          (.put wms table-name (.openWatermark live-table true)))

        (reify ILiveIndexWatermark
          (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))

          (liveTable [_ table-name] (.get wms table-name))

          AutoCloseable
          (close [_]
            (util/close wms)
            (.release wm-cnt))))
      (catch Throwable _t
        (.release wm-cnt))))

  (finishChunk [_ chunk-idx next-chunk-idx]
    (let [trie-key (trie/->log-trie-key 0 chunk-idx next-chunk-idx)
          futs (->> (for [^ILiveTable table (.values tables)]
                      (.finishChunk table trie-key))

                    (remove nil?)
                    (into-array CompletableFuture))]

      @(CompletableFuture/allOf futs)

      (-> (into {} (keep deref) futs)
          (util/rethrowing-cause))))

  (nextChunk [_]
    (util/close tables)
    (.clear tables))

  AutoCloseable
  (close [_]
    (util/close tables)
    (if-not (.tryClose wm-cnt (Duration/ofMinutes 1))
      (log/warn "Failed to shut down live-index after 60s due to outstanding watermarks.")
      (util/close allocator))))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :buffer-pool (ig/ref :xtdb/buffer-pool)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator buffer-pool log-limit page-limit]
                                                    :or {log-limit 64 page-limit 1024}}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
    (->LiveIndex allocator buffer-pool (HashMap.) (RefCounter.) log-limit page-limit)))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))
