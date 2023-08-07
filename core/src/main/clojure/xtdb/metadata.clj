(ns xtdb.metadata
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.expression.temporal
            xtdb.object-store
            [xtdb.transit :as xt.transit]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang Keyword MapEntry]
           (java.io ByteArrayInputStream ByteArrayOutputStream)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.util HashMap HashSet Map NavigableMap Set TreeMap)
           (java.util.concurrent ConcurrentHashMap)
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function BiFunction Consumer Function)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector FieldVector IntVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           (org.roaringbitmap RoaringBitmap)
           xtdb.buffer_pool.IBufferPool
           xtdb.object_store.ObjectStore
           (xtdb.vector IVectorReader IVectorWriter RelationReader ValueVectorReader$DuvReader)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITableMetadata
  (^org.apache.arrow.vector.VectorSchemaRoot metadataRoot [])
  (^java.util.Set columnNames [])
  (^Long rowIndex [^String column-name, ^int blockIdx]
   "pass blockIdx = -1 for metadata about the whole chunk")
  (^long blockCount []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IPageMetadataWriter
  (^void writeMetadata [^Iterable cols]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataManager
  (^void finishChunk [^long chunkIdx, newChunkMetadata])
  (^java.util.NavigableMap chunksMetadata [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^String tableName, ^java.util.function.Function #_<ITableMetadata> f])
  (columnTypes [^String tableName])
  (columnType [^String tableName, ^String colName])
  (allColumnTypes []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataPredicate
  (^java.util.function.IntPredicate build [^xtdb.metadata.ITableMetadata tableMetadata]))

(defrecord ChunkMatch [^long chunk-idx, ^RoaringBitmap block-idxs, ^Set col-names])

(defn- with-single-root [^IBufferPool buffer-pool, obj-key, f]
  (-> (.getBuffer buffer-pool obj-key)
      (util/then-apply
        (fn [^ArrowBuf buffer]
          (assert buffer)

          (when buffer
            (let [res (promise)]
              (try
                (with-open [chunk (util/->chunks buffer)]
                  (.tryAdvance chunk
                               (reify Consumer
                                 (accept [_ vsr]
                                   (deliver res (f vsr))))))

                (assert (realized? res))
                @res

                (finally
                  (.close buffer)))))))))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"chunk-metadata/(\p{XDigit}+).transit.json" obj-key))
          (util/<-lex-hex-string)))

(defn- ->chunk-metadata-obj-key [chunk-idx]
  (format "chunk-metadata/%s.transit.json" (util/->lex-hex-string chunk-idx)))

(defn- write-chunk-metadata ^java.nio.ByteBuffer [chunk-meta]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :json {:handlers xt.transit/tj-write-handlers})]
      (transit/write w chunk-meta))
    (ByteBuffer/wrap (.toByteArray os))))

(defn- merge-col-types [col-types {:keys [tables]}]
  (reduce (fn [col-types [table {new-col-types :col-types}]]
            (update col-types table
                    (fn [col-types new-col-types]
                      (merge-with types/merge-col-types col-types new-col-types))
                    new-col-types))
          col-types
          tables))

(def metadata-col-type
  '[:list
    [:struct
     {col-name :utf8
      root-col? :bool
      count :i64
      types [:struct {}]
      bloom [:union #{:null :varbinary}]}]])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface NestedMetadataWriter
  (^xtdb.metadata.ContentMetadataWriter appendNestedMetadata [^xtdb.vector.IVectorReader contentCol]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (types/col-type-head col-type))
  :hierarchy #'types/col-type-hierarchy)

(defn- ->bool-type-handler [^IVectorWriter types-wtr, col-type]
  (let [bit-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :bool}])]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.writeBoolean bit-wtr true)))))))

(defn- ->min-max-type-handler [^IVectorWriter types-wtr, col-type]
  ;; we get vectors out here because this code was largely written pre writers.
  (let [types-wp (.writerPosition types-wtr)

        struct-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type)
                                     [:union #{:null
                                               [:struct {'min [:union #{:null col-type}]
                                                         'max [:union #{:null col-type}]}]}])

        min-wtr (.structKeyWriter struct-wtr "min")
        ^FieldVector min-vec (.getVector min-wtr)
        min-wp (.writerPosition min-wtr)

        max-wtr (.structKeyWriter struct-wtr "max")
        ^FieldVector max-vec (.getVector max-wtr)
        max-wp (.writerPosition max-wtr)]

    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.startStruct struct-wtr)

            (let [pos (.getPosition types-wp)
                  min-copier (.rowCopier content-col min-wtr)
                  max-copier (.rowCopier content-col max-wtr)

                  min-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr min-wtr) :nulls-last)
                  max-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr max-wtr) :nulls-first)]

              (.writeNull min-wtr nil)
              (.writeNull max-wtr nil)

              (dotimes [value-idx (.valueCount content-col)]
                (when (or (.isNull min-vec pos)
                          (and (not (= :null (.getLeg content-col value-idx)))
                               (neg? (.applyAsInt min-comparator value-idx pos))))
                  (.setPosition min-wp pos)
                  (.copyRow min-copier value-idx))

                (when (or (.isNull max-vec pos)
                          (and (not (= :null (.getLeg content-col value-idx)))
                               (pos? (.applyAsInt max-comparator value-idx pos))))
                  (.setPosition max-wp pos)
                  (.copyRow max-copier value-idx)))

              (.endStruct struct-wtr))))))))


(doseq [type-head #{:null :bool :fixed-size-binary :uuid :clj-form}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type)))

(doseq [type-head #{:int :float :utf8 :varbinary :keyword :uri
                    :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod type->metadata-writer :list [write-col-meta! ^IVectorWriter types-wtr col-type]
  (let [types-wp (.writerPosition types-wtr)
        list-type-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :i32}])]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (write-col-meta! (.listElementReader ^IVectorReader content-col))

        (let [data-meta-idx (dec (.getPosition types-wp))]
          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.writeInt list-type-wtr data-meta-idx))))))))

(defmethod type->metadata-writer :struct [write-col-meta! ^IVectorWriter types-wtr, col-type]
  (let [types-wp (.writerPosition types-wtr)
        struct-type-wtr (.structKeyWriter types-wtr
                                          (str (types/col-type->field-name col-type) "-" (count (seq types-wtr)))
                                          [:union #{:null [:list [:union #{:null :i32}]]}])
        struct-type-el-wtr (.listElementWriter struct-type-wtr)]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (let [struct-keys (.structKeys content-col)
              sub-col-idxs (IntStream/builder)]

          (doseq [^String struct-key struct-keys]
            (write-col-meta! (.structKeyReader content-col struct-key))
            (.add sub-col-idxs (dec (.getPosition types-wp))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.startList struct-type-wtr)
              (doseq [sub-col-idx (.toArray (.build sub-col-idxs))]
                (.writeInt struct-type-el-wtr sub-col-idx))
              (.endList struct-type-wtr))))))))

(defn ->page-meta-wtr ^xtdb.metadata.IPageMetadataWriter [^IVectorWriter cols-wtr]
  (let [col-wtr (.listElementWriter cols-wtr)
        col-name-wtr (.structKeyWriter col-wtr "col-name")
        root-col-wtr (.structKeyWriter col-wtr "root-col?")
        count-wtr (.structKeyWriter col-wtr "count")
        types-wtr (.structKeyWriter col-wtr "types")
        bloom-wtr (.structKeyWriter col-wtr "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(->nested-meta-writer [^IVectorReader content-col]
              (let [col-type (first (-> (types/field->col-type (.getField content-col))
                                        (types/flatten-union-types)
                                        (disj :null)
                                        (doto (-> count (= 1) (assert "should just be nullable mono-vecs here")))))]
                (-> ^NestedMetadataWriter
                    (.computeIfAbsent type-metadata-writers col-type
                                      (reify Function
                                        (apply [_ col-type]
                                          (type->metadata-writer (partial write-col-meta! false) types-wtr col-type))))
                    (.appendNestedMetadata (.metadataReader content-col)))))

            (write-col-meta! [root-col?, ^IVectorReader content-col]
              (let [content-writers (if (instance? ArrowType$Union (.getType (.getField content-col)))
                                      (->> (.legs content-col)
                                           (mapv (comp ->nested-meta-writer #(.legReader content-col %))))
                                      [(->nested-meta-writer content-col)])]

                (.startStruct col-wtr)
                (.writeBoolean root-col-wtr root-col?)
                (.writeObject col-name-wtr (.getName content-col))
                (.writeLong count-wtr (.valueCount content-col))
                (bloom/write-bloom bloom-wtr content-col)

                (.startStruct types-wtr)
                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer))
                (.endStruct types-wtr)

                (.endStruct col-wtr)))]

      (reify IPageMetadataWriter
        (writeMetadata [_ cols]
          (.startList cols-wtr)
          (doseq [^IVectorReader col cols
                  :when (pos? (.valueCount col))]
            (write-col-meta! true col))
          (.endList cols-wtr))))))

(deftype MetadataManager [^ObjectStore object-store
                          ^NavigableMap chunks-metadata
                          ^:volatile-mutable ^Map col-types]
  IMetadataManager
  (finishChunk [this chunk-idx new-chunk-metadata]
    (-> @(.putObject object-store (->chunk-metadata-obj-key chunk-idx) (write-chunk-metadata new-chunk-metadata))
        (util/rethrowing-cause))
    (set! (.col-types this) (merge-col-types col-types new-chunk-metadata))
    (.put chunks-metadata chunk-idx new-chunk-metadata))

  (withMetadata [_ chunk-idx table-name f]
    (throw (UnsupportedOperationException.))
    #_
    (with-single-root buffer-pool (->table-metadata-obj-key chunk-idx table-name)
      (fn [metadata-root]
        (.apply f (->table-metadata metadata-root
                                    (.computeIfAbsent table-metadata-idxs
                                                      [chunk-idx table-name]
                                                      (reify Function
                                                        (apply [_ _]
                                                          (->table-metadata-idxs metadata-root)))))))))

  (chunksMetadata [_] chunks-metadata)

  (columnType [_ table-name col-name] (get-in col-types [table-name col-name]))
  (columnTypes [_ table-name] (get col-types table-name))
  (allColumnTypes [_] col-types)

  AutoCloseable
  (close [_]
    (.clear chunks-metadata)))

(defn latest-chunk-metadata [^IMetadataManager metadata-mgr]
  (some-> (.lastEntry (.chunksMetadata metadata-mgr))
          (.getValue)))

(defn- get-bytes ^java.util.concurrent.CompletableFuture #_<bytes> [^IBufferPool buffer-pool, obj-key]
  (-> (.getBuffer buffer-pool obj-key)
      (util/then-apply
        (fn [^ArrowBuf buffer]
          (assert buffer)

          (try
            (let [bb (.nioBuffer buffer 0 (.capacity buffer))
                  ba (byte-array (.remaining bb))]
              (.get bb ba)
              ba)
            (finally
              (.close buffer)))))))

(defn- load-chunks-metadata ^java.util.NavigableMap [{:keys [buffer-pool ^ObjectStore object-store]}]
  (let [cm (TreeMap.)]
    (doseq [cm-obj-key (.listObjects object-store "chunk-metadata/")]
      (with-open [is (ByteArrayInputStream. @(get-bytes buffer-pool cm-obj-key))]
        (let [rdr (transit/reader is :json {:handlers xt.transit/tj-read-handlers})]
          (.put cm (obj-key->chunk-idx cm-obj-key) (transit/read rdr)))))
    cm))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:object-store (ig/ref :xtdb/object-store)
          :buffer-pool (ig/ref :xtdb.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [^ObjectStore object-store], :as deps}]
  (let [chunks-metadata (load-chunks-metadata deps)]
    (MetadataManager. object-store
                      chunks-metadata
                      (->> (vals chunks-metadata) (reduce merge-col-types {})))))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, ^String table-name, ^Function f]
  (throw (UnsupportedOperationException.))
  #_ ; TODO reinstate when we bring back metadata into scan
  (.withMetadata metadata-mgr chunk-idx table-name f))

(defn with-all-metadata [^IMetadataManager metadata-mgr, table-name, ^BiFunction f]
  (throw (UnsupportedOperationException.))
  #_ ; TODO reinstate when we bring back metadata into scan
  (->> (for [[^long chunk-idx, chunk-metadata] (.chunksMetadata metadata-mgr)
             :let [table (get-in chunk-metadata [:tables table-name])]
             :when table]
         (with-metadata metadata-mgr chunk-idx table-name
           (util/->jfn
             (fn [table-meta]
               (.apply f chunk-idx table-meta)))))
       vec
       (into [] (keep deref))
       (util/rethrowing-cause)))

(defn matching-chunks [^IMetadataManager metadata-mgr, table-name, ^IMetadataPredicate metadata-pred]
  (throw (UnsupportedOperationException.))
  #_ ; TODO reinstate when we bring back metadata into scan
  (with-all-metadata metadata-mgr table-name
    (util/->jbifn
      (fn [^long chunk-idx, ^ITableMetadata table-metadata]
        (let [pred (.build metadata-pred table-metadata)]
          (when (.test pred -1)
            (let [block-idxs (RoaringBitmap.)]
              (dotimes [block-idx (.blockCount table-metadata)]
                (when (.test pred block-idx)
                  (.add block-idxs block-idx)))

              (when-not (.isEmpty block-idxs)
                (->ChunkMatch chunk-idx block-idxs (.columnNames table-metadata))))))))))

