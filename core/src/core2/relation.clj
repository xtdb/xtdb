(ns core2.relation
  (:require [core2.types :as ty]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           [core2.relation IAppendColumn IAppendRelation IReadColumn IReadRelation IRowAppender]
           java.lang.AutoCloseable
           [java.util LinkedHashMap Map]
           java.util.function.Function
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Field]))

(declare ->IndirectVectorBackedColumn)

(defrecord DirectVectorBackedColumn [^ValueVector v, ^String name]
  IReadColumn
  (getVector [_] v)
  (getIndex [_ idx] idx)
  (getName [_] name)
  (getValueCount [_] (.getValueCount v))

  (withName [_ name] (->DirectVectorBackedColumn v name))
  (select [_ idxs] (->IndirectVectorBackedColumn v name idxs))

  (copy [_ allocator]
    ;; we'd like to use .getTransferPair here but DUV is broken again
    ;; - it doesn't pass the fieldType through so you get a DUV with empty typeIds
    (let [to (-> (doto (.makeTransferPair v (.createVector (.getField v) allocator))
                   (.splitAndTransfer 0 (.getValueCount v)))
                 (.getTo))]
      (DirectVectorBackedColumn. to name))))

(defrecord IndirectVectorBackedColumn [^ValueVector v, ^String col-name, ^ints idxs]
  IReadColumn
  (getVector [_] v)
  (getIndex [_ idx] (aget idxs idx))
  (getName [_] col-name)
  (getValueCount [_] (alength idxs))

  (withName [_ col-name] (IndirectVectorBackedColumn. v col-name idxs))

  (select [this idxs]
    (let [^ints old-idxs (.idxs this)
          new-idxs (IntStream/builder)]
      (dotimes [idx (alength idxs)]
        (.add new-idxs (aget old-idxs (aget idxs idx))))

      (IndirectVectorBackedColumn. v col-name (.toArray (.build new-idxs)))))

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

      (DirectVectorBackedColumn. (doto (.getTo tp)
                                   (.setValueCount (alength idxs)))
                                 col-name))))

(defn ^core2.relation.IReadColumn <-vector
  ([^ValueVector in-vec]
   (DirectVectorBackedColumn. in-vec (.getName in-vec)))

  ([^ValueVector in-vec, ^ints idxs]
   (IndirectVectorBackedColumn. in-vec (.getName in-vec) idxs)))

(deftype ReadRelation [^Map cols, ^int row-count]
  IReadRelation
  (readColumn [_ col-name] (.get cols col-name))
  (rowCount [_] row-count)

  (iterator [_] (.iterator (.values cols)))

  (close [_] (run! util/try-close (.values cols))))

(defn ->read-relation ^core2.relation.IReadRelation [read-cols]
  (ReadRelation. (let [cols (LinkedHashMap.)]
                   (doseq [^IReadColumn col read-cols]
                     (.put cols (.getName col) col))
                   cols)
                 (if (seq read-cols)
                   (.getValueCount ^IReadColumn (first read-cols))
                   0)))

(defn <-root [^VectorSchemaRoot root]
  (let [cols (LinkedHashMap.)]
    (doseq [^ValueVector in-vec (.getFieldVectors root)]
      (.put cols (.getName in-vec) (<-vector in-vec)))
    (ReadRelation. cols (.getRowCount root))))

(defn select ^core2.relation.IReadRelation [^IReadRelation in-rel, ^ints idxs]
  (->read-relation (for [^IReadColumn in-col in-rel]
                     (.select in-col idxs))))

(defn copy ^core2.relation.IReadRelation [^IReadRelation in-rel, ^BufferAllocator allocator]
  (->read-relation (for [^IReadColumn in-col in-rel]
                     (.copy in-col allocator))))

(defn rel->rows ^java.lang.Iterable [^IReadRelation rel]
  (let [ks (for [^IReadColumn col rel]
             (keyword (.getName col)))]
    (mapv (fn [idx]
            (zipmap ks
                    (for [^IReadColumn col rel]
                      (ty/get-object (.getVector col) (.getIndex col idx)))))
          (range (.rowCount rel)))))

(deftype NestedReadColumn [^IReadColumn parent-col
                           ^byte type-id
                           ^ValueVector type-vec]
  IReadColumn
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

(defn nested-read-col ^core2.relation.IReadColumn [^IReadColumn col, ^ArrowType arrow-type]
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

(defn col->arrow-types [^IReadColumn col]
  (let [col-vec (.getVector col)
        field (.getField col-vec)]
    (if (instance? DenseUnionVector col-vec)
      (into #{} (for [^ValueVector vv (.getChildrenFromFields ^DenseUnionVector col-vec)
                      :when (pos? (.getValueCount vv))]
                  (.getType (.getField vv))))
      #{(.getType field)})))

(defn- duv->duv-appender ^core2.relation.IRowAppender [^BufferAllocator allocator, ^IReadColumn src-col, ^DenseUnionVector dest-duv]
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

(defn- vec->duv-appender ^core2.relation.IRowAppender [^BufferAllocator allocator, ^IReadColumn src-col, ^DenseUnionVector dest-duv]
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

(defn ->append-col [^BufferAllocator allocator, ^String col-name]
  (let [append-vec (DenseUnionVector/empty col-name allocator)]
    (reify IAppendColumn
      (appendColumn [this src-col]
        (let [row-appender (.rowAppender this src-col)]
          (dotimes [src-idx (.getValueCount src-col)]
            (.appendRow row-appender src-idx))))

      (rowAppender [_ src-col]
        (if (instance? DenseUnionVector (.getVector src-col))
          (duv->duv-appender allocator src-col append-vec)
          (vec->duv-appender allocator src-col append-vec)))

      (read [_] (<-vector append-vec))
      (clear [_] (.clear append-vec))
      (close [_] (.close append-vec)))))

(defn ->append-relation ^core2.relation.IAppendRelation [^BufferAllocator allocator]
  (let [append-cols (LinkedHashMap.)]
    (reify IAppendRelation
      (appendColumn [_ col-name]
        (.computeIfAbsent append-cols col-name
                          (reify Function
                            (apply [_ col-name]
                              (->append-col allocator col-name)))))

      (appendRelation [this src-rel]
        (doseq [^IReadColumn src-col src-rel
                :let [^IAppendColumn append-col (.appendColumn this (.getName src-col))]]
          (.appendColumn append-col src-col)))

      (read [_]
        (->read-relation (for [^IAppendColumn col (.values append-cols)]
                           (.read col))))

      (clear [_]
        (doseq [^IAppendColumn col (.values append-cols)]
          (.clear col)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values append-cols))))))
