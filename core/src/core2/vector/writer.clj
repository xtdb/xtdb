(ns core2.vector.writer
  (:require [core2.relation :as rel]
            [core2.types :as ty]
            [core2.util :as util])
  (:import [core2.relation IColumnReader IRelationReader]
           [core2.vector IDenseUnionWriter IListWriter IRelationWriter IRowCopier IStructWriter IVectorWriter]
           java.lang.AutoCloseable
           [java.util HashMap LinkedHashMap Map]
           java.util.function.Function
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.util.AutoCloseables
           [org.apache.arrow.vector NullVector ValueVector]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Field FieldType]))

(deftype DuvChildWriter [^IDenseUnionWriter parent-writer,
                         ^byte type-id
                         ^IVectorWriter inner-writer]
  IVectorWriter
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

(defn- duv->duv-copier ^core2.vector.IRowCopier [^IColumnReader src-col, ^IDenseUnionWriter dest-col]
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
                                                     (rel/reader-for-type-id src-col src-type-id)))))

    (reify IRowCopier
      (getWriter [_] dest-col)
      (getReader [_] src-col)

      (copyRow [_ src-idx]
        (-> ^IRowCopier (aget copier-mapping (.getTypeId src-vec (.getIndex src-col src-idx)))
            (.copyRow src-idx))))))

(defn- vec->duv-copier ^core2.vector.IRowCopier [^IColumnReader src-col, ^IDenseUnionWriter dest-col]
  (-> (.writerForType dest-col (-> (.getVector src-col) (.getField) (.getType)))
      (.rowCopier src-col)))

(declare ^core2.vector.IVectorWriter vec->writer)

(deftype DuvWriter [^DenseUnionVector dest-duv,
                    ^"[Lcore2.vector.IVectorWriter;" writers-by-type-id
                    ^Map writers-by-type
                    ^:unsynchronized-mutable ^int pos]
  IVectorWriter
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
        (.endValue ^IVectorWriter (aget writers-by-type-id type-id))))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (doseq [^IVectorWriter writer writers-by-type-id
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
                                type-id (or (rel/duv-type-id dest-duv arrow-type)
                                            (.registerNewTypeId dest-duv field))]
                            (when-not (.getVectorByType dest-duv type-id)
                              (.addVector dest-duv type-id
                                          (.createVector field (.getAllocator dest-duv))))

                            (.writerForTypeId this type-id)))))))

(deftype StructWriter [^StructVector dest-vec, ^Map writers,
                       ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [_]
    (.setIndexDefined dest-vec pos)
    (doseq [^IVectorWriter writer (vals writers)]
      (.startValue writer))
    pos)

  (endValue [this]
    (doseq [^IVectorWriter writer (vals writers)]
      (.endValue writer))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (doseq [^IVectorWriter writer (vals writers)]
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
                     ^IVectorWriter data-writer
                     ^:unsynchronized-mutable ^int pos
                     ^:unsynchronized-mutable ^int data-start-pos]
  IVectorWriter
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
  IVectorWriter
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

(defn ^core2.vector.IVectorWriter vec->writer
  ([^ValueVector dest-vec] (vec->writer dest-vec (.getValueCount dest-vec)))
  ([^ValueVector dest-vec, ^long pos]
   (cond
     (instance? DenseUnionVector dest-vec)
     (DuvWriter. dest-vec (make-array IVectorWriter (inc Byte/MAX_VALUE)) (HashMap.) pos)

     (instance? StructVector dest-vec)
     (StructWriter. dest-vec (HashMap.) pos)

     (instance? ListVector dest-vec)
     (ListWriter. dest-vec (vec->writer (.getDataVector ^ListVector dest-vec)) pos 0)

     :else
     (ScalarWriter. dest-vec pos))))

(defn ->rel-writer ^core2.vector.IRelationWriter [^BufferAllocator allocator]
  (let [vec-writers (LinkedHashMap.)]
    (reify IRelationWriter
      (writerForName [_ vec-name]
        (.computeIfAbsent vec-writers vec-name
                          (reify Function
                            (apply [_ vec-name]
                              (vec->writer (DenseUnionVector/empty vec-name allocator))))))

      (iterator [_]
        (.iterator (.values vec-writers)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values vec-writers))))))

(defn vec-writer->reader ^core2.relation.IColumnReader [^IVectorWriter vec-writer]
  (rel/vec->reader (.getVector vec-writer)))

(defn rel-writer->reader ^core2.relation.IRelationReader [^IRelationWriter rel-writer]
  (rel/->read-relation (for [^IVectorWriter vec-writer rel-writer]
                         (vec-writer->reader vec-writer))))

(defn append-vec [^IVectorWriter vec-writer, ^IColumnReader col-reader]
  (let [row-copier (.rowCopier vec-writer col-reader)]
    (dotimes [src-idx (.getValueCount col-reader)]
      (.startValue vec-writer)
      (.copyRow row-copier src-idx)
      (.endValue vec-writer))))

(defn append-rel [^IRelationWriter dest-rel, ^IRelationReader src-rel]
  (doseq [^IColumnReader src-col src-rel
          :let [^IVectorWriter vec-writer (.writerForName dest-rel (.getName src-col))]]
    (append-vec vec-writer src-col)))

(defn clear-rel [^IRelationWriter writer]
  (doseq [^IVectorWriter vec-writer writer]
    (.clear (.getVector vec-writer))
    (.clear vec-writer)))
