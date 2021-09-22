(ns core2.relation
  (:require [core2.types :as ty]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           [core2.relation IColumnReader IColumnWriter IRelationReader IRelationWriter IRowAppender]
           java.lang.AutoCloseable
           [java.util HashMap LinkedHashMap Map]
           java.util.function.Function
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Field FieldType]))

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

(defn ^core2.relation.IColumnReader vec->reader
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
      (.put cols (.getName in-vec) (vec->reader in-vec)))
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

(deftype DuvChildReader [^IColumnReader parent-col
                         ^DenseUnionVector parent-duv
                         ^byte type-id
                         ^ValueVector type-vec]
  IColumnReader
  (getVector [_] type-vec)
  (getIndex [_ idx] (.getOffset parent-duv (.getIndex parent-col idx)))
  (getName [_] (.getName parent-col))
  (withName [_ name] (DuvChildReader. (.withName parent-col name) parent-duv type-id type-vec)))

(defn reader-for-type-id ^core2.relation.IColumnReader [^IColumnReader col, type-id]
  (let [^DenseUnionVector parent-duv (.getVector col)]
    (DuvChildReader. col parent-duv type-id (.getVectorByType parent-duv type-id))))

(defn duv-type-id ^java.lang.Byte [^DenseUnionVector duv, ^ArrowType arrow-type]
  (let [field (.getField duv)
        type-ids (.getTypeIds ^ArrowType$Union (.getType field))]
    (-> (keep-indexed (fn [idx ^Field sub-field]
                        (when (= arrow-type (.getType sub-field))
                          (aget type-ids idx)))
                      (.getChildren field))
        (first))))

(defn reader-for-type ^core2.relation.IColumnReader [^IColumnReader col, ^ArrowType arrow-type]
  (let [v (.getVector col)
        field (.getField v)
        v-type (.getType field)]
    (cond
      (= arrow-type v-type) col

      (instance? DenseUnionVector v)
      (let [type-id (or (duv-type-id v arrow-type)
                        (throw (UnsupportedOperationException.)))
            type-vec (.getVectorByType ^DenseUnionVector v type-id)]
        (DuvChildReader. col v type-id type-vec)))))

(defn col->arrow-types [^IColumnReader col]
  (let [col-vec (.getVector col)
        field (.getField col-vec)]
    (if (instance? DenseUnionVector col-vec)
      (into #{} (for [^ValueVector vv (.getChildrenFromFields ^DenseUnionVector col-vec)
                      :when (pos? (.getValueCount vv))]
                  (.getType (.getField vv))))
      #{(.getType field)})))

(deftype DuvChildWriter [^IColumnWriter parent-writer,
                         ^DenseUnionVector parent-duv
                         ^byte type-id
                         ^ValueVector child-vec]
  IColumnWriter
  (getVector [_] child-vec)

  (appendIndex [this]
    (.appendIndex this (.appendIndex parent-writer)))

  (appendIndex [_ parent-idx]
    (DenseUnionUtil/writeTypeId parent-duv parent-idx type-id))

  (rowAppender [this src-col]
    (let [src-vec (.getVector src-col)]
      (reify IRowAppender
        (appendRow [this src-idx]
          (.appendRow this src-idx (.appendIndex parent-writer)))

        (appendRow [_ src-idx parent-idx]
          (.copyFromSafe child-vec
                         (.getIndex src-col src-idx)
                         (.appendIndex this parent-idx)
                         src-vec))))))

(defn- duv->duv-appender ^core2.relation.IRowAppender [^IColumnReader src-col, ^IColumnWriter dest-col]
  (let [^DenseUnionVector src-vec (.getVector src-col)
        src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        type-id-count (inc (apply max -1 type-ids))
        appender-mapping (object-array type-id-count)]

    (dotimes [n (alength type-ids)]
      (let [src-type-id (aget type-ids n)
            arrow-type (-> ^Field (.get (.getChildren src-field) n)
                           (.getType))]
        (aset appender-mapping src-type-id (.rowAppender (.writerForType dest-col arrow-type)
                                                         (reader-for-type-id src-col src-type-id)))))

    (reify IRowAppender
      (appendRow [_ src-idx]
        (-> ^IRowAppender (aget appender-mapping (.getTypeId src-vec (.getIndex src-col src-idx)))
            (.appendRow src-idx)))

      (appendRow [_ src-idx parent-idx]
        (-> ^IRowAppender (aget appender-mapping (.getTypeId src-vec (.getIndex src-col src-idx)))
            (.appendRow src-idx parent-idx))))))

(defn- vec->duv-appender ^core2.relation.IRowAppender [^IColumnReader src-col, ^IColumnWriter dest-col]
  (-> (.writerForType dest-col (-> (.getVector src-col) (.getField) (.getType)))
      (.rowAppender src-col)))

(deftype DuvWriter [^DenseUnionVector dest-duv, ^Map writers]
  IColumnWriter
  (getVector [_] dest-duv)

  (appendIndex [_]
    (let [idx (.getValueCount dest-duv)]
      (util/set-value-count dest-duv (inc idx))
      idx))

  (rowAppender [this src-col]
    (if (instance? DenseUnionVector (.getVector src-col))
      (duv->duv-appender src-col this)
      (vec->duv-appender src-col this)))

  (writerForTypeId [this type-id]
    (let [inner-vec (or (.getVectorByType dest-duv type-id)
                        (throw (IllegalArgumentException.)))]
      (DuvChildWriter. this dest-duv type-id inner-vec)))

  (writerForType [this arrow-type]
    (.computeIfAbsent writers arrow-type
                      (reify Function
                        (apply [_ arrow-type]
                          (let [^Field field (ty/->field (ty/type->field-name arrow-type) arrow-type false)
                                type-id (or (duv-type-id dest-duv arrow-type)
                                            (.registerNewTypeId dest-duv field))]
                            (when-not (.getVectorByType dest-duv type-id)
                              (.addVector dest-duv type-id
                                          (.createVector field (.getAllocator dest-duv))))

                            (.writerForTypeId this type-id)))))))

(declare vec->writer)

(deftype StructWriter [^StructVector dest-vec, ^Map writers]
  IColumnWriter
  (getVector [_] dest-vec)

  (appendIndex [_]
    (let [idx (.getValueCount dest-vec)]
      (util/set-value-count dest-vec (inc idx))
      (.setIndexDefined dest-vec idx)
      idx))

  (writerForName [_ col-name]
    (.computeIfAbsent writers col-name
                      (reify Function
                        (apply [_ col-name]
                          (-> (doto (.addOrGet dest-vec col-name
                                               (FieldType/nullable ty/dense-union-type)
                                               DenseUnionVector)
                                (util/set-value-count (.getValueCount dest-vec)))
                              (vec->writer)))))))

(defn vec->writer ^core2.relation.IColumnWriter [^ValueVector dest-vec]
  (cond
    (instance? DenseUnionVector dest-vec) (DuvWriter. dest-vec (HashMap.))
    (instance? StructVector dest-vec) (StructWriter. dest-vec (HashMap.))
    :else (reify IColumnWriter
            (getVector [_] dest-vec)

            (appendIndex [_]
              (let [idx (.getValueCount dest-vec)]
                (util/set-value-count dest-vec (inc idx))
                idx)))))

(defn ->rel-writer ^core2.relation.IRelationWriter [^BufferAllocator allocator]
  (let [col-writers (LinkedHashMap.)]
    (reify IRelationWriter
      (columnWriter [_ col-name]
        (.computeIfAbsent col-writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (vec->writer (DenseUnionVector/empty col-name allocator))))))

      (iterator [_]
        (.iterator (.values col-writers)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values col-writers))))))

(defn col-writer->reader ^core2.relation.IColumnReader [^IColumnWriter col-writer]
  (vec->reader (.getVector col-writer)))

(defn rel-writer->reader ^core2.relation.IRelationReader [^IRelationWriter rel-writer]
  (->read-relation (for [^IColumnWriter col-writer rel-writer]
                     (col-writer->reader col-writer))))

(defn append-col [^IColumnWriter col-writer, ^IColumnReader col-reader]
  (let [row-appender (.rowAppender col-writer col-reader)]
    (dotimes [src-idx (.getValueCount col-reader)]
      (.appendRow row-appender src-idx))))

(defn append-rel [^IRelationWriter dest-rel, ^IRelationReader src-rel]
  (doseq [^IColumnReader src-col src-rel
          :let [^IColumnWriter col-writer (.columnWriter dest-rel (.getName src-col))]]
    (append-col col-writer src-col)))

(defn clear-col [^IColumnWriter writer]
  (.clear (.getVector writer)))

(defn clear-rel [^IRelationWriter writer]
  (run! clear-col writer))
