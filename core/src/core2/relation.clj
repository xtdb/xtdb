(ns core2.relation
  (:require [core2.types :as ty]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           [core2.relation IColumnReader IColumnWriter IDenseUnionWriter IListWriter IRelationReader IRelationWriter IRowCopier IStructWriter]
           java.lang.AutoCloseable
           [java.util HashMap LinkedHashMap Map]
           java.util.function.Function
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables
           [org.apache.arrow.vector NullVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
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
                        (throw (IllegalArgumentException.)))
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

(deftype DuvChildWriter [^IDenseUnionWriter parent-writer,
                         ^byte type-id
                         ^IColumnWriter inner-writer]
  IColumnWriter
  (getVector [_] (.getVector inner-writer))
  (getPosition [_] (.getPosition inner-writer))

  (asStruct [duv-child-writer]
    (let [^IStructWriter inner-writer (cast IStructWriter inner-writer)] ; cast to throw CCE early
      (reify
        IStructWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (writerForName [_ col-name] (.writerForName inner-writer col-name))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer)))))

  (asList [duv-child-writer]
    (let [^IListWriter inner-writer (cast IListWriter inner-writer)] ; cast to throw CCE early
      (reify
        IListWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer))
        (getDataWriter [_] (.getDataWriter inner-writer)))))

  (startValue [this]
    (let [parent-duv (.getVector parent-writer)
          parent-pos (.getPosition parent-writer)
          pos (.getPosition this)]
      (.setTypeId parent-duv parent-pos type-id)
      (.setOffset parent-duv parent-pos pos)

      (util/set-value-count parent-duv (inc parent-pos))
      (util/set-value-count (.getVector this) (inc pos))

      (.startValue inner-writer)

      pos))

  (endValue [_] (.endValue inner-writer))
  (clear [_] (.clear inner-writer))

  (rowCopier [this-writer src-col]
    (let [inner-copier (.rowCopier inner-writer src-col)]
      (reify IRowCopier
        (getWriter [_] this-writer)
        (getReader [_] src-col)

        (copyRow [this src-idx]
          (.startValue this-writer)
          (.copyRow inner-copier src-idx))))))

(defn- duv->duv-copier ^core2.relation.IRowCopier [^IColumnReader src-col, ^IDenseUnionWriter dest-col]
  (let [^DenseUnionVector src-vec (.getVector src-col)
        src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        type-id-count (inc (apply max -1 type-ids))
        copier-mapping (object-array type-id-count)]

    (dotimes [n (alength type-ids)]
      (let [src-type-id (aget type-ids n)
            arrow-type (-> ^Field (.get (.getChildren src-field) n)
                           (.getType))]
        (aset copier-mapping src-type-id (.rowCopier (.writerForType dest-col arrow-type)
                                                     (reader-for-type-id src-col src-type-id)))))

    (reify IRowCopier
      (getWriter [_] dest-col)
      (getReader [_] src-col)

      (copyRow [_ src-idx]
        (-> ^IRowCopier (aget copier-mapping (.getTypeId src-vec (.getIndex src-col src-idx)))
            (.copyRow src-idx))))))

(defn- vec->duv-copier ^core2.relation.IRowCopier [^IColumnReader src-col, ^IDenseUnionWriter dest-col]
  (-> (.writerForType dest-col (-> (.getVector src-col) (.getField) (.getType)))
      (.rowCopier src-col)))

(declare ^core2.relation.IColumnWriter vec->writer)

(deftype DuvWriter [^DenseUnionVector dest-duv,
                    ^"[Lcore2.relation.IColumnWriter;" writers-by-type-id
                    ^Map writers-by-type
                    ^:unsynchronized-mutable ^int pos]
  IColumnWriter
  (getVector [_] dest-duv)
  (getPosition [_] pos)

  (startValue [_]
    ;; allocates memory in the type-id/offset buffers even if the DUV value is null
    (.setTypeId dest-duv pos -1)
    (.setOffset dest-duv pos 0)
    pos)

  (endValue [this]
    (let [type-id (.getTypeId dest-duv pos)]
      (when-not (neg? type-id)
        (.endValue ^IColumnWriter (aget writers-by-type-id type-id))))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (doseq [^IColumnWriter writer writers-by-type-id
            :when writer]
      (.clear writer))
    (set! (.pos this) 0))

  (rowCopier [this-writer src-col]
    (let [inner-copier (if (instance? DenseUnionVector (.getVector src-col))
                         (duv->duv-copier src-col this-writer)
                         (vec->duv-copier src-col this-writer))]
      (reify IRowCopier
        (getWriter [_] this-writer)
        (getReader [_] src-col)
        (copyRow [_ src-idx] (.copyRow inner-copier src-idx)))))

  (asDenseUnion [this] this)

  IDenseUnionWriter
  (writerForTypeId [this type-id]
    (or (aget writers-by-type-id type-id)
        (let [inner-vec (or (.getVectorByType dest-duv type-id)
                            (throw (IllegalArgumentException.)))
              inner-writer (vec->writer inner-vec)
              writer (DuvChildWriter. this type-id inner-writer)]
          (aset writers-by-type-id type-id writer)
          writer)))

  (writerForType [this arrow-type]
    (.computeIfAbsent writers-by-type arrow-type
                      (reify Function
                        (apply [_ arrow-type]
                          (let [^Field field (ty/->field (ty/type->field-name arrow-type) arrow-type false)
                                type-id (or (duv-type-id dest-duv arrow-type)
                                            (.registerNewTypeId dest-duv field))]
                            (when-not (.getVectorByType dest-duv type-id)
                              (.addVector dest-duv type-id
                                          (.createVector field (.getAllocator dest-duv))))

                            (.writerForTypeId this type-id)))))))

(deftype StructWriter [^StructVector dest-vec, ^Map writers,
                       ^:unsynchronized-mutable ^int pos]
  IColumnWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [_]
    (.setIndexDefined dest-vec pos)
    (doseq [^IColumnWriter writer (vals writers)]
      (.startValue writer))
    pos)

  (endValue [this]
    (doseq [^IColumnWriter writer (vals writers)]
      (.endValue writer))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (doseq [^IColumnWriter writer (vals writers)]
      (.clear writer))
    (set! (.pos this) 0))

  (asStruct [this] this)

  IStructWriter
  (writerForName [_ col-name]
    (.computeIfAbsent writers col-name
                      (reify Function
                        (apply [_ col-name]
                          (-> (or (.getChild dest-vec col-name)
                                  (.addOrGet dest-vec col-name
                                             (FieldType/nullable ty/dense-union-type)
                                             DenseUnionVector))
                              (vec->writer pos)))))))

(deftype ListWriter [^ListVector dest-vec
                     ^IColumnWriter data-writer
                     ^:unsynchronized-mutable ^int pos
                     ^:unsynchronized-mutable ^int data-start-pos]
  IColumnWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)
  (asList [this] this)

  (startValue [this]
    (set! (.data-start-pos this) (.startNewValue dest-vec pos))
    pos)

  (endValue [this]
    (.endValue dest-vec pos (- (.getPosition data-writer) data-start-pos))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear data-writer)
    (set! (.pos this) 0))

  IListWriter
  (getDataWriter [_] data-writer))

(deftype ScalarWriter [^ValueVector dest-vec,
                       ^:unsynchronized-mutable ^int pos]
  IColumnWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)
  (startValue [_] pos)
  (endValue [this] (set! (.pos this) (inc pos)))

  (clear [this] (set! (.pos this) 0))

  (rowCopier [this-writer src-col]
    (let [src-vec (.getVector src-col)]
      (if (instance? NullVector dest-vec)
        ;; `NullVector/.copyFromSafe` throws UOE
        (reify IRowCopier
          (getWriter [_] this-writer)
          (getReader [_] src-col)
          (copyRow [_ src-idx]))

        (reify IRowCopier
          (getWriter [_] this-writer)
          (getReader [_] src-col)
          (copyRow [_ src-idx]
            (.copyFromSafe dest-vec
                           (.getIndex src-col src-idx)
                           (.getPosition this-writer)
                           src-vec)))))))

(defn ^core2.relation.IColumnWriter vec->writer
  ([^ValueVector dest-vec] (vec->writer dest-vec (.getValueCount dest-vec)))
  ([^ValueVector dest-vec, ^long pos]
   (cond
     (instance? DenseUnionVector dest-vec)
     (DuvWriter. dest-vec (make-array IColumnWriter (inc Byte/MAX_VALUE)) (HashMap.) pos)

     (instance? StructVector dest-vec)
     (StructWriter. dest-vec (HashMap.) pos)

     (instance? ListVector dest-vec)
     (ListWriter. dest-vec (vec->writer (.getDataVector ^ListVector dest-vec)) pos 0)

     :else
     (ScalarWriter. dest-vec pos))))

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
  (let [row-copier (.rowCopier col-writer col-reader)]
    (dotimes [src-idx (.getValueCount col-reader)]
      (.startValue col-writer)
      (.copyRow row-copier src-idx)
      (.endValue col-writer))))

(defn append-rel [^IRelationWriter dest-rel, ^IRelationReader src-rel]
  (doseq [^IColumnReader src-col src-rel
          :let [^IColumnWriter col-writer (.columnWriter dest-rel (.getName src-col))]]
    (append-col col-writer src-col)))

(defn clear-rel [^IRelationWriter writer]
  (doseq [^IColumnWriter col-writer writer]
    (.clear (.getVector col-writer))
    (.clear col-writer)))
