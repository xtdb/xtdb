(ns core2.vector.writer
  (:require [clojure.core.match :refer [match]]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (core2.vector IDenseUnionWriter IExtensionWriter IIndirectRelation IIndirectVector IListWriter IRelationWriter IRowCopier IStructWriter IVectorWriter)
           (java.lang AutoCloseable)
           (java.util HashMap LinkedHashMap Map)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.util AutoCloseables)
           (org.apache.arrow.vector ExtensionTypeVector NullVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Struct ArrowType$Union Field FieldType)))

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

  (asExtension [duv-child-writer]
    (let [^IExtensionWriter inner-writer (cast IExtensionWriter inner-writer)] ; cast to throw CCE early
      (reify
        IExtensionWriter
        (getVector [_] (.getVector inner-writer))
        (getPosition [_] (.getPosition inner-writer))
        (startValue [_] (.startValue duv-child-writer))
        (endValue [_] (.endValue duv-child-writer))
        (getUnderlyingWriter [_] (.getUnderlyingWriter inner-writer)))))

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
        (copyRow [_this src-idx]
          (.startValue this-writer)
          (.copyRow inner-copier src-idx)
          (.endValue this-writer))))))

(defn- duv->duv-copier ^core2.vector.IRowCopier [^DenseUnionVector src-vec, ^IDenseUnionWriter dest-col]
  (let [src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        type-id-count (inc (apply max -1 type-ids))
        copier-mapping (object-array type-id-count)]

    (dotimes [n (alength type-ids)]
      (let [src-type-id (aget type-ids n)
            col-type (types/field->col-type (.get (.getChildren src-field) n))]
        (aset copier-mapping src-type-id (.rowCopier (.writerForType dest-col col-type)
                                                     (.getVectorByType src-vec src-type-id)))))

    (reify IRowCopier
      (copyRow [_ src-idx]
        (let [type-id (.getTypeId src-vec src-idx)]
          (when-not (neg? type-id)
            (-> ^IRowCopier (aget copier-mapping type-id)
                (.copyRow (.getOffset src-vec src-idx)))))))))

(defn- vec->duv-copier ^core2.vector.IRowCopier [^ValueVector src-vec, ^IDenseUnionWriter dest-col]
  (let [field (.getField src-vec)
        col-type (types/field->col-type field)]
    (match col-type
      [:union inner-types]
      (let [without-null (disj inner-types :null)]
        (assert (= 1 (count without-null)))
        (let [nn-col-type (first without-null)
              non-null-copier (-> (.writerForType dest-col nn-col-type)
                                  (.rowCopier src-vec))
              null-copier (-> (.writerForType dest-col :null)
                              (.rowCopier src-vec))]
          (reify IRowCopier
            (copyRow [_ src-idx]
              (if (.isNull src-vec src-idx)
                (.copyRow null-copier src-idx)
                (.copyRow non-null-copier src-idx))))))

      :else
      (-> (.writerForType dest-col col-type)
          (.rowCopier src-vec)))))

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
    (when (neg? (.getTypeId dest-duv pos))
      (doto (.writerForType this :null)
        (.startValue)
        (.endValue)))

    (set! (.pos this) (inc pos)))

  (clear [this]
    (doseq [^IVectorWriter writer writers-by-type-id
            :when writer]
      (.clear writer))
    (set! (.pos this) 0))

  (rowCopier [this-writer src-vec]
    (let [inner-copier (if (instance? DenseUnionVector src-vec)
                         (duv->duv-copier src-vec this-writer)
                         (vec->duv-copier src-vec this-writer))]
      (reify IRowCopier
        (copyRow [_ src-idx] (.copyRow inner-copier src-idx)))))

  IDenseUnionWriter
  (writerForTypeId [this type-id]
    (or (aget writers-by-type-id type-id)
        (let [^ValueVector
              inner-vec (or (.getVectorByType dest-duv type-id)
                            (throw (IllegalArgumentException.)))
              inner-writer (vec->writer inner-vec)
              writer (DuvChildWriter. this type-id inner-writer)]
          (aset writers-by-type-id type-id writer)
          writer)))

  (writerForType [this col-type]
    (.computeIfAbsent writers-by-type (match col-type
                                        [:struct inner-types] [:struct (set (keys inner-types))]
                                        [:list _inner-type] :list
                                        :else col-type)
                      (reify Function
                        (apply [_ _]
                          (let [field-name (types/col-type->field-name col-type)

                                ^Field field (case (types/col-type-head col-type)
                                               :list
                                               (types/->field field-name ArrowType$List/INSTANCE false (types/->field "$data$" types/dense-union-type false))

                                               :struct
                                               (types/->field (str field-name (count writers-by-type)) ArrowType$Struct/INSTANCE false)

                                               (types/col-type->field field-name col-type))

                                type-id (or (iv/duv-type-id dest-duv col-type)
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

  (rowCopier [struct-writer src-vec]
    (let [^StructVector src-vec (cast StructVector src-vec)
          copiers (vec (for [^ValueVector child-vec (.getChildrenFromFields src-vec)]
                         (.rowCopier (.writerForName struct-writer (.getName child-vec))
                                     child-vec)))]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (doseq [^IRowCopier copier copiers]
            (.copyRow copier src-idx))))))

  IStructWriter
  (writerForName [_ col-name]
    (.computeIfAbsent writers col-name
                      (reify Function
                        (apply [_ col-name]
                          (-> (or (.getChild dest-vec col-name)
                                  (.addOrGet dest-vec col-name
                                             (FieldType/notNullable types/dense-union-type)
                                             DenseUnionVector))
                              (vec->writer pos)))))))

(deftype ListWriter [^ListVector dest-vec
                     ^IVectorWriter data-writer
                     ^:unsynchronized-mutable ^int pos
                     ^:unsynchronized-mutable ^int data-start-pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [this]
    (set! (.data-start-pos this) (.startNewValue dest-vec pos))
    pos)

  (endValue [this]
    (.endValue dest-vec pos (- (.getPosition data-writer) data-start-pos))
    (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear data-writer)
    (set! (.pos this) 0))

  (rowCopier [_ src-vec]
    (let [^ListVector src-vec (cast ListVector src-vec)
          src-data-vec (.getDataVector src-vec)
          inner-copier (.rowCopier data-writer src-data-vec)]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [start-idx (.getElementStartIndex src-vec src-idx)]
            (dotimes [el-idx (- (.getElementEndIndex src-vec src-idx) start-idx)]
              (.startValue data-writer)
              (.copyRow inner-copier (+ start-idx el-idx))
              (.endValue data-writer)))))))

  IListWriter
  (getDataWriter [_] data-writer))

(deftype FixedSizeListWriter [^FixedSizeListVector dest-vec
                              ^IVectorWriter data-writer
                              ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)

  (startValue [_] pos)
  (endValue [this] (set! (.pos this) (inc pos)))

  (clear [this]
    (.clear data-writer)
    (set! (.pos this) 0))

  (rowCopier [_ src-vec]
    (let [^FixedSizeListVector src-vec (cast FixedSizeListVector src-vec)
          src-data-vec (.getDataVector src-vec)
          inner-copier (.rowCopier data-writer src-data-vec)]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [start-idx (.getElementStartIndex src-vec src-idx)]
            (dotimes [el-idx (- (.getElementEndIndex src-vec src-idx) start-idx)]
              (.startValue data-writer)
              (.copyRow inner-copier (+ start-idx el-idx))
              (.endValue data-writer)))))))

  IListWriter
  (getDataWriter [_] data-writer))

(deftype ExtensionWriter [^ExtensionTypeVector dest-vec, ^IVectorWriter underlying-writer]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] (.getPosition underlying-writer))
  (startValue [_] (.startValue underlying-writer))
  (endValue [_] (.endValue underlying-writer))

  (clear [_] (.clear underlying-writer))

  (rowCopier [_ src-vec]
    (.rowCopier underlying-writer (.getUnderlyingVector ^ExtensionTypeVector src-vec)))

  IExtensionWriter
  (getUnderlyingWriter [_] underlying-writer))

(deftype ScalarWriter [^ValueVector dest-vec,
                       ^:unsynchronized-mutable ^int pos]
  IVectorWriter
  (getVector [_] dest-vec)
  (getPosition [_] pos)
  (startValue [_] pos)
  (endValue [this] (set! (.pos this) (inc pos)))

  (clear [this] (set! (.pos this) 0))

  (rowCopier [this-writer src-vec]
    (if (instance? NullVector dest-vec)
      ;; `NullVector/.copyFromSafe` throws UOE
      (reify IRowCopier
        (copyRow [_ _src-idx]))

      (reify IRowCopier
        (copyRow [_ src-idx]
          (.copyFromSafe dest-vec
                         src-idx
                         (.getPosition this-writer)
                         src-vec))))))

(defn vec->writer
  (^core2.vector.IVectorWriter
   [^ValueVector dest-vec] (vec->writer dest-vec (.getValueCount dest-vec)))

  (^core2.vector.IVectorWriter
   [^ValueVector dest-vec, ^long pos]
   (cond
     (instance? DenseUnionVector dest-vec)
     ;; eugh. we have to initialise the writers map if the DUV is already populated.
     ;; easiest way to do this (that I can see) is to re-use `.writerForTypeId`.
     ;; if I'm making a dog's dinner of this, feel free to refactor.
     (let [writers-by-type (HashMap.)
           writer (DuvWriter. dest-vec
                              (make-array IVectorWriter (inc Byte/MAX_VALUE))
                              writers-by-type
                              pos)]
       (dotimes [writer-idx (count (seq dest-vec))]
         (let [inner-writer (.writerForTypeId writer writer-idx)
               inner-vec (.getVector inner-writer)
               duv-leg-key (-> (types/field->col-type (.getField inner-vec))
                               types/col-type->duv-leg-key)]
           (.put writers-by-type duv-leg-key inner-writer)))

       writer)

     (instance? StructVector dest-vec)
     (StructWriter. dest-vec (HashMap.) pos)

     (instance? ListVector dest-vec)
     (ListWriter. dest-vec (vec->writer (.getDataVector ^ListVector dest-vec)) pos 0)

     (instance? FixedSizeListVector dest-vec)
     (FixedSizeListWriter. dest-vec (vec->writer (.getDataVector ^FixedSizeListVector dest-vec)) pos)

     (instance? ExtensionTypeVector dest-vec)
     (ExtensionWriter. dest-vec (vec->writer (.getUnderlyingVector ^ExtensionTypeVector dest-vec)))

     :else
     (ScalarWriter. dest-vec pos))))

(defn ->vec-writer ^core2.vector.IVectorWriter [^BufferAllocator allocator, ^String col-name]
  (vec->writer (-> (types/->field col-name types/dense-union-type false)
                   (.createVector allocator))))

(defn ->rel-writer ^core2.vector.IRelationWriter [^BufferAllocator allocator]
  (let [writers (LinkedHashMap.)]
    (reify IRelationWriter
      (writerForName [_ col-name]
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (->vec-writer allocator col-name)))))

      (iterator [_]
        (.iterator (.values writers)))

      AutoCloseable
      (close [_]
        (AutoCloseables/close (.values writers))))))

(defn rel-writer->reader ^core2.vector.IIndirectRelation [^IRelationWriter rel-writer]
  (iv/->indirect-rel (for [^IVectorWriter vec-writer rel-writer]
                       (iv/->direct-vec (.getVector vec-writer)))))

(defn ->row-copier ^core2.vector.IRowCopier [^IVectorWriter vec-writer, ^IIndirectVector in-col]
  (let [in-vec (.getVector in-col)
        row-copier (.rowCopier vec-writer in-vec)]
    (reify IRowCopier
      (copyRow [_ src-idx]
        (.startValue vec-writer)
        (.copyRow row-copier (.getIndex in-col src-idx))
        (.endValue vec-writer)))))

(defn append-vec [^IVectorWriter vec-writer, ^IIndirectVector in-col]
  (let [row-copier (->row-copier vec-writer in-col)]
    (dotimes [src-idx (.getValueCount in-col)]
      (.copyRow row-copier src-idx))))

(defn append-rel [^IRelationWriter dest-rel, ^IIndirectRelation src-rel]
  (doseq [^IIndirectVector src-col src-rel
          :let [^IVectorWriter vec-writer (.writerForName dest-rel (.getName src-col))]]
    (append-vec vec-writer src-col)))
