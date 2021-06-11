(ns core2.relation
  (:require [core2.types :as types]
            [core2.util :as util])
  (:import [core2.relation IAppendColumn IAppendRelation IReadColumn IReadRelation]
           java.nio.ByteBuffer
           java.time.Duration
           [java.util ArrayList Collection Date Set HashMap HashSet LinkedHashMap List Map Map$Entry Set]
           java.util.function.Function
           [java.util.stream IntStream IntStream$Builder]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BaseVariableWidthVector BigIntVector BitVector DurationVector Float8Vector NullVector TimeStampMilliVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType Field FieldType]))

(definterface IAppendColumnPrivate
  (^org.apache.arrow.vector.ValueVector _getAppendVector [^org.apache.arrow.vector.types.pojo.ArrowType arrowType])
  (^int _getAppendIndex [^org.apache.arrow.vector.ValueVector appendVector]))

(defn- element->nio-buffer ^java.nio.ByteBuffer [^BaseVariableWidthVector vec ^long idx]
  (let [value-buffer (.getDataBuffer vec)
        offset-buffer (.getOffsetBuffer vec)
        offset-idx (* idx BaseVariableWidthVector/OFFSET_WIDTH)
        offset (.getInt offset-buffer offset-idx)
        end-offset (.getInt offset-buffer (+ offset-idx BaseVariableWidthVector/OFFSET_WIDTH))]
    (.nioBuffer value-buffer offset (- end-offset offset))))

(deftype ReadColumn [^String col-name
                     ^Set arrow-types
                     ^Collection close-vecs
                     ^"[Lorg.apache.arrow.vector.ValueVector;" vecs
                     ^ints idxs
                     ^int value-count]
  IReadColumn
  (getName [_] col-name)
  (rename [_ col-name] (ReadColumn. col-name arrow-types #{} vecs idxs value-count))
  (valueCount [_] value-count)
  (arrowTypes [_] arrow-types)

  (getBool [_ idx] (= 1 (.get ^BitVector (aget vecs idx) (aget idxs idx))))
  (getDouble [_ idx] (.get ^Float8Vector (aget vecs idx) (aget idxs idx)))
  (getLong [_ idx] (.get ^BigIntVector (aget vecs idx) (aget idxs idx)))
  (getDateMillis [_ idx] (.get ^TimeStampMilliVector (aget vecs idx) (aget idxs idx)))
  (getDurationMillis [_ idx] (DurationVector/get (.getDataBuffer ^DurationVector (aget vecs idx)) (aget idxs idx)))
  (getBuffer [_ idx] (element->nio-buffer (aget vecs idx) (aget idxs idx)))
  (getObject [_ idx] (types/get-object (aget vecs idx) (aget idxs idx)))

  (_getInternalVector [_ idx] (aget vecs idx))
  (_getInternalIndex [_ idx] (aget idxs idx))

  (close [_]
    (run! util/try-close close-vecs)))

(defn- ->read-column [^String col-name, arrow-types, ^Collection close-vecs, ^List vecs, ^IntStream$Builder idxs]
  (let [idxs (.toArray (.build idxs))
        value-count (alength idxs)
        vecs (.toArray vecs ^"[Lorg.apache.arrow.vector.ValueVector;" (make-array ValueVector value-count))]
    (ReadColumn. col-name (into #{} arrow-types) close-vecs vecs idxs value-count)))

(deftype DirectVectorBackedColumn [^ValueVector in-vec, ^String col-name, ^Set arrow-types]
  IReadColumn
  (getName [_] col-name)
  (rename [_ col-name] (DirectVectorBackedColumn. in-vec col-name arrow-types))
  (valueCount [_] (.getValueCount in-vec))
  (arrowTypes [_] arrow-types)

  (getBool [_ idx] (= 1 (.get ^BitVector in-vec idx)))
  (getDouble [_ idx] (.get ^Float8Vector in-vec idx))
  (getLong [_ idx] (.get ^BigIntVector in-vec idx))
  (getDateMillis [_ idx] (.get ^TimeStampMilliVector in-vec idx))
  (getDurationMillis [_ idx] (DurationVector/get (.getDataBuffer ^DurationVector in-vec) idx))
  (getBuffer [_ idx] (element->nio-buffer in-vec idx))
  (getObject [_ idx] (types/get-object in-vec idx))

  (_getInternalVector [_ _idx] in-vec)
  (_getInternalIndex [_ idx] idx)

  (close [_]
    (util/try-close in-vec)))

(deftype DenseUnionColumn [^DenseUnionVector in-vec, ^String col-name, ^Set arrow-types]
  IReadColumn
  (getName [_] col-name)
  (rename [_ col-name] (DenseUnionColumn. in-vec col-name arrow-types))
  (valueCount [_] (.getValueCount in-vec))
  (arrowTypes [_] arrow-types)

  (getBool [this idx]
    (= 1 (.get ^BitVector (._getInternalVector this idx)
               (._getInternalIndex this idx))))

  (getDouble [this idx]
    (-> ^Float8Vector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getLong [this idx]
    (-> ^BigIntVector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getDateMillis [this idx]
    (-> ^TimeStampMilliVector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getDurationMillis [this idx]
    (-> ^DurationVector (._getInternalVector this idx)
        (.getDataBuffer)
        (DurationVector/get (._getInternalIndex this idx))))

  (getBuffer [this idx]
    (-> (._getInternalVector this idx)
        (element->nio-buffer (._getInternalIndex this idx))))

  (getObject [_ idx] (types/get-object in-vec idx))

  (_getInternalVector [_ idx] (.getVectorByType in-vec (.getTypeId in-vec idx)))
  (_getInternalIndex [_ idx] (.getOffset in-vec idx))

  (close [_]
    (util/try-close in-vec)))

(deftype IndirectVectorBackedColumn [^ValueVector in-vec, ^String col-name, ^Set arrow-types, ^ints idxs]
  IReadColumn
  (getName [_] col-name)
  (rename [_ col-name] (IndirectVectorBackedColumn. in-vec col-name arrow-types idxs))
  (valueCount [_] (alength idxs))
  (arrowTypes [_] arrow-types)

  (getBool [_ idx] (= 1 (.get ^BitVector in-vec (aget idxs idx))))
  (getDouble [_ idx] (.get ^Float8Vector in-vec (aget idxs idx)))
  (getLong [_ idx] (.get ^BigIntVector in-vec (aget idxs idx)))
  (getDateMillis [_ idx] (.get ^TimeStampMilliVector in-vec (aget idxs idx)))
  (getDurationMillis [_ idx] (DurationVector/get (.getDataBuffer ^DurationVector in-vec) (aget idxs idx)))
  (getBuffer [_ idx] (element->nio-buffer in-vec (aget idxs idx)))
  (getObject [_ idx] (types/get-object in-vec (aget idxs idx)))

  (_getInternalVector [_ _idx] in-vec)
  (_getInternalIndex [_ idx] (aget idxs idx))

  (close [_]
    (util/try-close in-vec)))

(deftype IndirectDenseUnionColumn [^DenseUnionVector in-vec, ^String col-name, ^Set arrow-types, ^ints idxs]
  IReadColumn
  (getName [_] col-name)
  (rename [_ col-name] (IndirectDenseUnionColumn. in-vec col-name arrow-types idxs))
  (arrowTypes [_] arrow-types)
  (valueCount [_] (alength idxs))

  (getBool [this idx]
    (= 1 (.get ^BitVector (._getInternalVector this idx)
               (._getInternalIndex this idx))))

  (getDouble [this idx]
    (-> ^Float8Vector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getLong [this idx]
    (-> ^BigIntVector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getDateMillis [this idx]
    (-> ^TimeStampMilliVector (._getInternalVector this idx)
        (.get (._getInternalIndex this idx))))

  (getDurationMillis [this idx]
    (-> ^DurationVector (._getInternalVector this idx)
        (.getDataBuffer)
        (DurationVector/get (._getInternalIndex this idx))))

  (getBuffer [this idx]
    (-> (._getInternalVector this idx)
        (element->nio-buffer (._getInternalIndex this idx))))

  (getObject [_ idx] (types/get-object in-vec (aget idxs idx)))

  (_getInternalVector [_ idx] (.getVectorByType in-vec (.getTypeId in-vec (aget idxs idx))))
  (_getInternalIndex [_ idx] (.getOffset in-vec (aget idxs idx)))

  (close [_]
    (util/try-close in-vec)))

(defn- vector-arrow-types [^ValueVector v]
  #{(.getType (.getField v))})

(defn- dense-union-arrow-types [^DenseUnionVector duv]
  (into #{} (for [^ValueVector vv (.getChildrenFromFields duv)
                  :when (pos? (.getValueCount vv))]
              (.getType (.getField vv)))))

(defn ^core2.relation.IReadColumn <-vector
  ([^ValueVector in-vec]
   (if (instance? DenseUnionVector in-vec)
     (DenseUnionColumn. in-vec (.getName in-vec) (dense-union-arrow-types in-vec))
     (DirectVectorBackedColumn. in-vec (.getName in-vec) (vector-arrow-types in-vec))))

  ([^ValueVector in-vec, ^ints idxs]
   (if (instance? DenseUnionVector in-vec)
     (IndirectDenseUnionColumn. in-vec (.getName in-vec) (dense-union-arrow-types in-vec) idxs)
     (IndirectVectorBackedColumn. in-vec (.getName in-vec) (vector-arrow-types in-vec) idxs))))

(deftype ReadRelation [^Map cols, ^int row-count]
  IReadRelation
  (readColumns [_] (.values cols))
  (readColumn [_ col-name] (.get cols col-name))
  (rowCount [_] row-count)

  (close [_]
    (run! util/try-close (.values cols))))

(defn <-root [^VectorSchemaRoot root]
  (let [cols (LinkedHashMap.)]
    (doseq [^ValueVector in-vec (.getFieldVectors root)]
      (.put cols (.getName in-vec) (<-vector in-vec)))
    (ReadRelation. cols (.getRowCount root))))

(defn append->read-cols [^Map append-cols]
  (let [read-cols (LinkedHashMap.)]
    (doseq [^Map$Entry kv append-cols]
      (.put read-cols (.getKey kv) (.read ^IAppendColumn (.getValue kv))))

    read-cols))

(defn ->read-relation
  ([^Map read-cols]
   (->read-relation read-cols
                    (if (seq read-cols)
                      (.valueCount ^IReadColumn (val (first read-cols)))
                      0)))

  ([^Map read-cols, ^long row-count]
   (ReadRelation. read-cols row-count)))

(deftype IndirectAppendColumn [^String col-name
                               ^Set arrow-types
                               ^Set vecSet
                               ^List vecs
                               ^:unsynchronized-mutable ^IntStream$Builder idxs]
  IAppendColumn
  (appendFrom [_ src idx]
    (let [in-vec (._getInternalVector src idx)]
      (.add arrow-types (.getType (.getField in-vec)))
      (.add vecs in-vec)
      (.add vecSet in-vec)
      (.add idxs (._getInternalIndex src idx))))

  (read [_]
    (->read-column col-name arrow-types #{} vecs idxs))

  (close [_]
    (.clear vecSet)
    (.clear vecs)))

(defn ->indirect-append-column [col-name]
  (IndirectAppendColumn. col-name (HashSet.) (util/->identity-set) (ArrayList.) (IntStream/builder)))

(deftype AppendRelation [^Map append-cols, ^Function col-fn]
  IAppendRelation
  (appendColumn [_ col-name]
    (.computeIfAbsent append-cols col-name col-fn))

  (read [_]
    (->read-relation (append->read-cols append-cols)))

  (close [_]
    (run! util/try-close (.values append-cols))))

(defn ->indirect-append-relation ^core2.relation.IAppendRelation []
  (AppendRelation. (LinkedHashMap.)
                   (reify Function
                     (apply [_ col-name]
                       (->indirect-append-column col-name)))))

(defn- append-object [^IAppendColumn col, obj]
  (case (types/arrow-type->type-id
         (types/class->arrow-type (class obj)))
    1 (.appendNull col)
    2 (.appendLong col obj)
    3 (.appendDouble col obj)
    4 (.appendBytes col obj)
    5 (.appendString col (ByteBuffer/wrap (.getBytes ^String obj)))
    6 (.appendBool col obj)
    10 (.appendDateMillis col (.getTime ^Date obj))
    18 (.appendDurationMillis col (.toMillis ^Duration obj))
    (throw (ex-info "can't append this" {:obj obj,
                                         :type (class obj),
                                         :arrow-type (types/class->arrow-type (class obj))
                                         :type-id (types/arrow-type->type-id
                                                   (types/class->arrow-type (class obj)))}))))

(deftype VectorBackedAppendColumn [^ValueVector out-vec]
  IAppendColumn
  (appendFrom [_ read-col idx]
    (let [^ValueVector in-vec (._getInternalVector read-col idx)
          out-idx (.getValueCount out-vec)]
      (.setValueCount out-vec (inc out-idx))
      (.copyFromSafe ^ValueVector out-vec (._getInternalIndex read-col idx) out-idx in-vec)))

  (read [_] (<-vector out-vec))

  (appendNull [this]
    ;; calling _getAppendIndex adds one to null's valueCount
    (._getAppendIndex this out-vec))

  (appendBool [this bool]
    (.set ^BitVector out-vec (._getAppendIndex this out-vec) (if bool 1 0)))

  (appendDouble [this dbl]
    (.set ^Float8Vector out-vec (._getAppendIndex this out-vec) dbl))

  (appendLong [this dbl]
    (.set ^BigIntVector out-vec (._getAppendIndex this out-vec) dbl))

  (appendString [this buf]
    (.setSafe ^VarCharVector out-vec (._getAppendIndex this out-vec) buf (.position buf) (.remaining buf)))

  (appendBytes [this buf]
    (.setSafe ^VarBinaryVector out-vec (._getAppendIndex this out-vec) buf (.position buf) (.remaining buf)))

  (appendDateMillis [this date]
    (.set ^TimeStampMilliVector out-vec (._getAppendIndex this out-vec) date))

  (appendDurationMillis [this duration]
    (.set ^DurationVector out-vec (._getAppendIndex this out-vec) duration))

  (appendObject [this obj]
    (append-object this obj))

  IAppendColumnPrivate
  (_getAppendIndex [_ out-vec]
    (let [idx (.getValueCount out-vec)]
      (.setValueCount out-vec (inc idx))
      idx)))

(defn ->vector-append-column ^IAppendColumn [^BufferAllocator allocator, ^String col-name, ^ArrowType arrow-type]
  (VectorBackedAppendColumn.
   (.createVector (Field. col-name true arrow-type nil nil)
                  allocator)))

(deftype FreshAppendColumn [^BufferAllocator allocator
                            ^String col-name
                            ^Set arrow-types
                            ^Map type->vecs
                            ^List vecs
                            ^IntStream$Builder idxs]
  IAppendColumn
  (appendFrom [this read-col idx]
    (let [in-vec (._getInternalVector read-col idx)
          out-vec (._getAppendVector this (.getType (.getField in-vec)))]
      (.copyFromSafe out-vec (._getInternalIndex read-col idx) (._getAppendIndex this out-vec) in-vec)))

  (read [_] (->read-column col-name arrow-types (vals type->vecs) vecs idxs))

  (appendNull [this]
    (let [^NullVector out-vec (._getAppendVector this (types/->arrow-type :null))]
      ;; calling _getAppendIndex adds one to null's valueCount
      (._getAppendIndex this out-vec)))

  (appendBool [this bool]
    (let [^BitVector out-vec (._getAppendVector this (types/->arrow-type :bit))]
      (.set out-vec (._getAppendIndex this out-vec) (if bool 1 0))))

  (appendDouble [this dbl]
    (let [^Float8Vector out-vec (._getAppendVector this (types/->arrow-type :float8))]
      (.set out-vec (._getAppendIndex this out-vec) dbl)))

  (appendLong [this dbl]
    (let [^BigIntVector out-vec (._getAppendVector this (types/->arrow-type :bigint))]
      (.set out-vec (._getAppendIndex this out-vec) dbl)))

  (appendString [this buf]
    (let [^VarCharVector out-vec (._getAppendVector this (types/->arrow-type :varchar))]
      (.setSafe out-vec (._getAppendIndex this out-vec) buf (.position buf) (.remaining buf))))

  (appendDateMillis [this date]
    (let [^TimeStampMilliVector out-vec (._getAppendVector this (types/->arrow-type :timestamp-milli))]
      (.set out-vec (._getAppendIndex this out-vec) date)))

  (appendDurationMillis [this duration]
    (let [^DurationVector out-vec (._getAppendVector this (types/->arrow-type :duration-milli))]
      (.set out-vec (._getAppendIndex this out-vec) duration)))

  (appendObject [this obj]
    (append-object this obj))

  IAppendColumnPrivate
  (_getAppendVector [_ arrow-type]
    (.add arrow-types arrow-type)
    (let [out-vec (.computeIfAbsent type->vecs
                                    arrow-type
                                    (reify Function
                                      (apply [_ arrow-type]
                                        (let [^ArrowType arrow-type arrow-type]
                                          (.createVector (Field. col-name true arrow-type nil nil)
                                                         allocator)))))]
      (.add vecs out-vec)
      out-vec))

  (_getAppendIndex [_ out-vec]
    (let [idx (.getValueCount out-vec)]
      (.setValueCount out-vec (inc idx))
      (.add idxs idx)
      idx))

  (close [_]
    (run! util/try-close (.values type->vecs))))

(defn ->fresh-append-column ^core2.relation.IAppendColumn [allocator col-name]
  (FreshAppendColumn. allocator col-name (HashSet.) (HashMap.) (ArrayList.) (IntStream/builder)))

(defn ->fresh-append-relation ^core2.relation.IAppendRelation [allocator]
  (AppendRelation. (LinkedHashMap.)
                   (reify Function
                     (apply [_ col-name]
                       (->fresh-append-column allocator col-name)))))

(defn select ^core2.relation.IReadRelation [^IReadRelation in-rel ^ints idxs]
  (let [append-rel (->indirect-append-relation)]
    (doseq [^IReadColumn read-col (.readColumns in-rel)
            :let [append-col (.appendColumn append-rel (.getName read-col))]]
      (dotimes [idx (alength idxs)]
        (.appendFrom append-col read-col (aget idxs idx))))
    (.read append-rel)))

(defn copy-col-from
  ([^IAppendColumn out-col, ^IReadColumn in-col, ^long offset, ^long length]
   ;; TODO batch?
   (dotimes [idx length]
     (.appendFrom out-col in-col (+ idx offset)))))

(defn copy-rel-from
  ([^IAppendRelation out-rel, ^IReadRelation in-rel]
   (copy-rel-from out-rel in-rel 0 (.rowCount in-rel)))

  ([^IAppendRelation out-rel, ^IReadRelation in-rel, ^long offset, ^long length]
   (doseq [^IReadColumn in-col (.readColumns in-rel)]
     (copy-col-from (.appendColumn out-rel (.getName in-col)) in-col
                    offset length))))

(definterface IRowCopier
  (appendRow [^int idx]))

(deftype RowCopier [^List in-cols, ^List out-cols]
  IRowCopier
  (appendRow [_ idx]
    (dorun
     (map (fn [^IAppendColumn out-col, ^IReadColumn in-col]
            (.appendFrom out-col in-col idx))
          out-cols in-cols))))

(defn row-copier ^core2.relation.IRowCopier [^IAppendRelation out-rel, ^IReadRelation in-rel]
  (let [in-cols (.readColumns in-rel)]
    (RowCopier. in-cols
                (mapv (fn [^IReadColumn col]
                        (.appendColumn out-rel (.getName col)))
                      in-cols))))

(defn rel->rows [^IReadRelation rel]
  (let [cols (.readColumns rel)
        ks (for [^IReadColumn col cols]
             (keyword (.getName col)))]
    (map (fn [idx]
           (zipmap ks
                   (for [^IReadColumn col cols]
                     (.getObject col idx))))
          (range (.rowCount rel)))))
