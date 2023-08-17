(ns xtdb.indexer.live-index
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.object-store]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           (java.lang AutoCloseable)
           (java.util ArrayList HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           [org.apache.arrow.vector.types.pojo Field]
           (xtdb.object_store ObjectStore)
           (xtdb.trie LiveHashTrie)
           (xtdb.vector IRelationWriter IVectorWriter RelationReader)))

;;
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableWatermark
  (^java.util.Map columnTypes [])
  (^xtdb.vector.RelationReader liveRelation [])
  (^xtdb.trie.LiveHashTrie liveTrie []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.indexer.live_index.ILiveTableWatermark openWatermark [])
  (^xtdb.vector.IVectorWriter docWriter [])
  (^void logPut [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo, writeDocFn])
  (^void logDelete [^java.nio.ByteBuffer iid, ^long validFrom, ^long validTo])
  (^void logEvict [^java.nio.ByteBuffer iid])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^xtdb.indexer.live_index.ILiveTableWatermark openWatermark [^boolean retain])
  (^java.util.concurrent.CompletableFuture #_<List<Map$Entry>> finishChunk [^long chunkIdx])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexWatermark
  (^java.util.Map allColumnTypes [])
  (^xtdb.indexer.live_index.ILiveTableWatermark liveTable [^String tableName]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexWatermark openWatermark [])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^xtdb.indexer.live_index.ILiveIndexWatermark openWatermark [])
  (^java.util.Map finishChunk [^long chunkIdx])
  (^void close []))

(defprotocol TestLiveTable
  (^xtdb.trie.LiveHashTrie live-trie [test-live-table])
  (^xtdb.vector.IRelationWriter live-rel [test-live-table]))

(defn- live-rel->col-types [^RelationReader live-rel]
  (->> (for [^Field child-field (-> (.readerForName live-rel "op")
                                    (.legReader :put)
                                    (.structKeyReader "xt$doc")
                                    (.getField)
                                    (.getChildren))]
         (MapEntry/create (.getName child-field) (types/field->col-type child-field)))
       (into {})))

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

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore obj-store, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^LiveHashTrie live-trie
                    ^IVectorWriter iid-wtr, ^IVectorWriter system-from-wtr
                    ^IVectorWriter put-wtr, ^IVectorWriter put-valid-from-wtr, ^IVectorWriter put-valid-to-wtr, ^IVectorWriter put-doc-wtr
                    ^IVectorWriter delete-wtr, ^IVectorWriter delete-valid-from-wtr, ^IVectorWriter delete-valid-to-wtr
                    ^IVectorWriter evict-wtr]
  ILiveTable
  (startTx [this-table tx-key]
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

          (.writeNull evict-wtr nil)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (openWatermark [_]
          (locking this-table
            (let [wm-live-rel (open-wm-live-rel live-rel false)
                  col-types (live-rel->col-types wm-live-rel)
                  wm-live-trie @!transient-trie]

              (reify ILiveTableWatermark
                (columnTypes [_] col-types)
                (liveRelation [_] wm-live-rel)
                (liveTrie [_] wm-live-trie)

                AutoCloseable
                (close [_])))))

        (commit [_]
          (locking this-table
            (set! (.-live-trie this-table) @!transient-trie)))

        AutoCloseable
        (close [_]))))

  (finishChunk [_ chunk-idx]
    (let [live-rel-rdr (vw/rel-wtr->rdr live-rel)]
      (when (pos? (.rowCount live-rel-rdr))
        (let [bufs (trie/live-trie->bufs allocator live-trie live-rel-rdr)
              chunk-idx-str (util/->lex-hex-string chunk-idx)
              !fut (trie/write-trie-bufs! obj-store table-name chunk-idx-str bufs)
              table-metadata (MapEntry/create table-name
                                              {:col-types (live-rel->col-types live-rel-rdr)
                                               :row-count (.rowCount live-rel-rdr)})]
          (-> !fut
              (util/then-apply (fn [_] table-metadata)))))))

  (openWatermark [this retain?]
    (locking this
      (let [wm-live-rel (open-wm-live-rel live-rel retain?)
            col-types (live-rel->col-types wm-live-rel)
            wm-live-trie (.withIidReader ^LiveHashTrie (.live-trie this) (.readerForName wm-live-rel "xt$iid"))]

        (reify ILiveTableWatermark
          (columnTypes [_] col-types)
          (liveRelation [_] wm-live-rel)
          (liveTrie [_] wm-live-trie)

          AutoCloseable
          (close [_]
            (when retain? (util/close wm-live-rel)))))))

  TestLiveTable
  (live-trie [_] live-trie)
  (live-rel [_] live-rel)

  AutoCloseable
  (close [this]
    (locking this
      (util/close live-rel))))

(defn ->live-table
  ([allocator object-store table-name] (->live-table allocator object-store table-name {}))

  ([allocator object-store table-name
    {:keys [->live-trie]
     :or {->live-trie (fn [iid-rdr]
                        (LiveHashTrie/emptyTrie iid-rdr))}}]

   (util/with-close-on-catch [rel (trie/open-leaf-root allocator)]
     (let [iid-wtr (.writerForName rel "xt$iid")
           op-wtr (.writerForName rel "op")
           put-wtr (.writerForTypeId op-wtr (byte 0))
           delete-wtr (.writerForTypeId op-wtr (byte 1))]
       (->LiveTable allocator object-store table-name rel
                    (->live-trie (vw/vec-wtr->rdr iid-wtr))
                    iid-wtr (.writerForName rel "xt$system_from")
                    put-wtr (.structKeyWriter put-wtr "xt$valid_from") (.structKeyWriter put-wtr "xt$valid_to")
                    (.structKeyWriter put-wtr "xt$doc") delete-wtr (.structKeyWriter delete-wtr "xt$valid_from")
                    (.structKeyWriter delete-wtr "xt$valid_to")
                    (.writerForTypeId op-wtr (byte 2)))))))

(defn ->live-trie [log-limit page-limit iid-rdr]
  (-> (doto (LiveHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defrecord LiveIndex [^BufferAllocator allocator, ^ObjectStore object-store, ^Map tables, ^long log-limit, ^long page-limit]
  ILiveIndex
  (liveTable [_ table-name]
    (.computeIfAbsent tables table-name
                      (reify Function
                        (apply [_ table-name]
                          (->live-table allocator object-store table-name
                                        {:->live-trie (partial ->live-trie log-limit page-limit)})))))

  (startTx [live-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (-> (.liveTable live-idx table-name)
                                    (.startTx tx-key))))))

        (commit [_]
          (doseq [^ILiveTableTx table-tx (.values table-txs)]
            (.commit table-tx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^ILiveTableTx live-table] table-txs]
              (.put wms table-name (.openWatermark live-table)))

            (doseq [[table-name ^ILiveTable live-table] tables]
              (.computeIfAbsent wms table-name
                                (util/->jfn (fn [_] (.openWatermark live-table false)))))

            (reify ILiveIndexWatermark
              (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_] (util/close wms)))))

        AutoCloseable
        (close [_]
          (util/close table-txs)))))

  (openWatermark [_]

    (util/with-close-on-catch [wms (HashMap.)]

      (doseq [[table-name ^ILiveTable live-table] tables]
        (.put wms table-name (.openWatermark live-table true)))

      (reify ILiveIndexWatermark
        (allColumnTypes [_] (update-vals wms #(.columnTypes ^ILiveTableWatermark %)))

        (liveTable [_ table-name] (.get wms table-name))

        AutoCloseable
        (close [_]
          (util/close wms)))))

  (finishChunk [_ chunk-idx]
    (let [futs (->> (for [^ILiveTable table (.values tables)]
                      (.finishChunk table chunk-idx))

                    (remove nil?)
                    (into-array CompletableFuture))]

      @(CompletableFuture/allOf futs)

      (util/close tables)
      (.clear tables)

      (-> (into {} (keep deref) futs)
          (util/rethrowing-cause))))

  AutoCloseable
  (close [_]
    (util/close tables)))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator object-store log-limit page-limit]
                                                    :or {log-limit 64 page-limit 1024}}]
  (->LiveIndex allocator object-store (HashMap.) log-limit page-limit))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))
