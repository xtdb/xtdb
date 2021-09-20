(ns core2.relation
  (:require [core2.types :as ty]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           [core2.relation IColumnWriter IRelationWriter IColumnReader IRelationReader IRowAppender]
           java.lang.AutoCloseable
           [java.util LinkedHashMap Map]
           java.util.function.Function
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Field]))

(declare ->IndirectVectorReader)

(defrecord DirectVectorReader [^ValueVector v, ^String name]
  IColumnReader
  (getVector [_] v)
  (getIndex [_ idx] idx)
  (getName [_] name)
  (getValueCount [_] (.getValueCount v))

  (withName [_ name] (->DirectVectorReader v name))
  (select [_ idxs] (->IndirectVectorReader v name idxs))

  (copy [_ allocator]
    ;; we'd like to use .getTransferPair here but DUV is broken again
    ;; - it doesn't pass the fieldType through so you get a DUV with empty typeIds
    (let [to (-> (doto (.makeTransferPair v (.createVector (.getField v) allocator))
                   (.splitAndTransfer 0 (.getValueCount v)))
                 (.getTo))]
      (DirectVectorReader. to name))))

(defrecord IndirectVectorReader [^ValueVector v, ^String col-name, ^ints idxs]
  IColumnReader
  (getVector [_] v)
  (getIndex [_ idx] (aget idxs idx))
  (getName [_] col-name)
  (getValueCount [_] (alength idxs))

  (withName [_ col-name] (IndirectVectorReader. v col-name idxs))

  (select [this idxs]
    (let [^ints old-idxs (.idxs this)
          new-idxs (IntStream/builder)]
      (dotimes [idx (alength idxs)]
        (.add new-idxs (aget old-idxs (aget idxs idx))))

      (IndirectVectorReader. v col-name (.toArray (.build new-idxs)))))

  (copy [_ allocator]
    (let [tp (.makeTransferPair v (.createVector (.getField v) allocator))]

      (if (instance? DenseUnionVector v)
        ;; DUV.copyValueSafe is broken - it's not safe, and it calls DenseUnionWriter.setPosition which NPEs
        (let [^DenseUnionVector from-duv v
              ^DenseUnionVector to-duv (.getTo tp)]
          (dotimes [idx (alength idxs)]
            (let [src-idx (aget idxs idx)
                  type-id (.getTypeId from-duv src-idx)
                  dest-offset (DenseUnionUtil/writeTypeId to-duv idx type-id)]
              (.copyFromSafe (.getVectorByType to-duv type-id)
                             (.getOffset from-duv src-idx)
                             dest-offset
                             (.getVectorByType from-duv type-id)))))

        (dotimes [idx (alength idxs)]
          (.copyValueSafe tp (aget idxs idx) idx)))

      (DirectVectorReader. (doto (.getTo tp)
                                   (.setValueCount (alength idxs)))
                                 col-name))))

(defn ^core2.relation.IColumnReader <-vector
  ([^ValueVector in-vec]
   (DirectVectorReader. in-vec (.getName in-vec)))

  ([^ValueVector in-vec, ^ints idxs]
   (IndirectVectorReader. in-vec (.getName in-vec) idxs)))

(deftype ReadRelation [^Map cols, ^int row-count]
  IRelationReader
  (columnReader [_ col-name] (.get cols col-name))
  (rowCount [_] row-count)

  (iterator [_] (.iterator (.values cols)))

  (close [_] (run! util/try-close (.values cols))))

(defn ->read-relation ^core2.relation.IRelationReader [read-cols]
  (ReadRelation. (let [cols (LinkedHashMap.)]
                   (doseq [^IColumnReader col read-cols]
                     (.put cols (.getName col) col))
                   cols)
                 (if (seq read-cols)
                   (.getValueCount ^IColumnReader (first read-cols))
                   0)))

(defn <-root [^VectorSchemaRoot root]
  (let [cols (LinkedHashMap.)]
    (doseq [^ValueVector in-vec (.getFieldVectors root)]
      (.put cols (.getName in-vec) (<-vector in-vec)))
    (ReadRelation. cols (.getRowCount root))))

(defn select ^core2.relation.IRelationReader [^IRelationReader in-rel, ^ints idxs]
  (->read-relation (for [^IColumnReader in-col in-rel]
                     (.select in-col idxs))))

(defn copy ^core2.relation.IRelationReader [^IRelationReader in-rel, ^BufferAllocator allocator]
  (->read-relation (for [^IColumnReader in-col in-rel]
                     (.copy in-col allocator))))

(defn rel->rows ^java.lang.Iterable [^IRelationReader rel]
  (let [ks (for [^IColumnReader col rel]
             (keyword (.getName col)))]
    (mapv (fn [idx]
            (zipmap ks
                    (for [^IColumnReader col rel]
                      (ty/get-object (.getVector col) (.getIndex col idx)))))
          (range (.rowCount rel)))))

(deftype NestedReadColumn [^IColumnReader parent-col
                           ^byte type-id
                           ^ValueVector type-vec]
  IColumnReader
  (getVector [_] type-vec)
  (getIndex [_ idx] (.getOffset ^DenseUnionVector (.getVector parent-col)
                                (.getIndex parent-col idx)))
  (getName [_] (.getName parent-col))
  (withName [_ name] (NestedReadColumn. (.withName parent-col name) type-id type-vec)))

(defn duv-type-id ^java.lang.Byte [^DenseUnionVector duv, ^ArrowType arrow-type]
  (let [field (.getField duv)
        type-ids (.getTypeIds ^ArrowType$Union (.getType field))]
    (-> (keep-indexed (fn [idx ^Field sub-field]
                        (when (= arrow-type (.getType sub-field))
                          (aget type-ids idx)))
                      (.getChildren field))
        (first))))

(defn nested-read-col ^core2.relation.IColumnReader [^IColumnReader col, ^ArrowType arrow-type]
  (let [v (.getVector col)
        field (.getField v)
        v-type (.getType field)]
    (cond
      (= arrow-type v-type) col

      (instance? DenseUnionVector v)
      (let [type-id (or (duv-type-id v arrow-type)
                        (throw (UnsupportedOperationException.)))
            type-vec (.getVectorByType ^DenseUnionVector v type-id)]
        (NestedReadColumn. col type-id type-vec)))))

(defn col->arrow-types [^IColumnReader col]
  (let [col-vec (.getVector col)
        field (.getField col-vec)]
    (if (instance? DenseUnionVector col-vec)
      (into #{} (for [^ValueVector vv (.getChildrenFromFields ^DenseUnionVector col-vec)
                      :when (pos? (.getValueCount vv))]
                  (.getType (.getField vv))))
      #{(.getType field)})))

(defn- duv->duv-appender ^core2.relation.IRowAppender [^BufferAllocator allocator, ^IColumnReader src-col, ^DenseUnionVector dest-duv]
  (let [^DenseUnionVector src-vec (.getVector src-col)
        src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        type-id-count (inc (apply max type-ids))
        type-id-mapping (byte-array type-id-count)

        ^"[Lorg.apache.arrow.vector.ValueVector;"
        dest-vec-mapping (make-array ValueVector type-id-count)]
    (dotimes [n (alength type-ids)]
      (let [src-type-id (aget type-ids n)
            ^Field nested-src-field (.get (.getChildren src-field) n)
            dest-type-id (or (duv-type-id dest-duv (.getType nested-src-field))
                             (.registerNewTypeId dest-duv nested-src-field))
            dest-vec (or (.getVectorByType dest-duv dest-type-id)
                         (.addVector dest-duv dest-type-id
                                     (.createVector nested-src-field allocator)))]
        (aset type-id-mapping src-type-id (byte dest-type-id))
        (aset dest-vec-mapping src-type-id dest-vec)))

    (reify IRowAppender
      (appendRow [_ src-idx]
        (let [src-type-id (.getTypeId src-vec (.getIndex src-col src-idx))
              dest-idx (DenseUnionUtil/writeTypeId dest-duv (.getValueCount dest-duv) (aget type-id-mapping src-type-id))]
          (.copyFromSafe ^ValueVector (aget dest-vec-mapping src-type-id)
                         (.getOffset src-vec (.getIndex src-col src-idx))
                         dest-idx
                         (.getVectorByType src-vec src-type-id)))))))

(defn- vec->duv-appender ^core2.relation.IRowAppender [^BufferAllocator allocator, ^IColumnReader src-col, ^DenseUnionVector dest-duv]
  (let [src-vec (.getVector src-col)
        src-field (.getField src-vec)
        src-type (.getType src-field)
        type-id (or (duv-type-id dest-duv src-type)
                    (.registerNewTypeId dest-duv src-field))
        ^ValueVector dest-vec (or (.getVectorByType dest-duv type-id)
                                  (.addVector dest-duv type-id (.createVector src-field allocator)))]
    (reify IRowAppender
      (appendRow [_ src-idx]
        (let [dest-idx (DenseUnionUtil/writeTypeId dest-duv (.getValueCount dest-duv) type-id)]
          (.copyFromSafe dest-vec (.getIndex src-col src-idx) dest-idx src-vec))))))

(defn ->col-writer [^BufferAllocator allocator, ^String col-name]
  (let [dest-vec (DenseUnionVector/empty col-name allocator)]
    (reify IColumnWriter
      (rowAppender [_ src-col]
        (if (instance? DenseUnionVector (.getVector src-col))
          (duv->duv-appender allocator src-col dest-vec)
          (vec->duv-appender allocator src-col dest-vec)))

      (read [_] (<-vector dest-vec))
      (clear [_] (.clear dest-vec))
      (close [_] (.close dest-vec)))))

(defn append-col [^IColumnWriter col-writer, ^IColumnReader col-reader]
  (let [row-appender (.rowAppender col-writer col-reader)]
    (dotimes [src-idx (.getValueCount col-reader)]
      (.appendRow row-appender src-idx))))

(defn ->rel-writer ^core2.relation.IRelationWriter [^BufferAllocator allocator]
  (let [col-writers (LinkedHashMap.)]
    (reify IRelationWriter
      (columnWriter [_ col-name]
        (.computeIfAbsent col-writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (->col-writer allocator col-name)))))

      (appendRelation [this src-rel]
        (doseq [^IColumnReader src-col src-rel
                :let [^IColumnWriter col-writer (.columnWriter this (.getName src-col))]]
          (append-col col-writer src-col)))

      (read [_]
        (->read-relation (for [^IColumnWriter col (.values col-writers)]
                           (.read col))))

      (clear [_]
        (doseq [^IColumnWriter col (.values col-writers)]
          (.clear col)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values col-writers))))))
