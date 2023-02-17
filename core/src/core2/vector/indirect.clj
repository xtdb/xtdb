(ns core2.vector.indirect
  (:require [core2.vector :as vec]
            [core2.types :as ty]
            [core2.util :as util])
  (:import core2.ICursor
           [core2.vector IIndirectRelation IIndirectVector IListElementCopier IListReader IRowCopier IStructReader]
           [java.util Iterator LinkedHashMap Map]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$Struct ArrowType$Union Field]))

(declare ^core2.vector.IIndirectVector ->direct-vec
         ^core2.vector.IIndirectVector ->IndirectVector)

(defrecord NullIndirectVector []
  IIndirectVector
  (isPresent [_ _] false)
  (rowCopier [_ w]
    (reify IRowCopier
      (copyRow [_ _]
        (.getPosition w)))))

(defrecord StructReader [^ValueVector v]
  IStructReader
  (structKeys [_]
    (letfn [(struct-keys [^Field field]
              (let [arrow-type (.getType field)]
                (cond
                  (instance? ArrowType$Struct arrow-type)
                  (->> (.getChildren field)
                       (into #{} (map #(.getName ^Field %))))

                  (instance? ArrowType$Union arrow-type)
                  (into #{} (mapcat struct-keys) (.getChildren field)))))]
      (struct-keys (.getField v))))

  (readerForKey [_ col-name]
    (letfn [(reader-for-key [^FieldVector v, ^String col-name]
              ;; TODO have only implemented a fraction of the required methods thus far
              (cond
                (instance? StructVector v)
                (if-let [child-vec (.getChild ^StructVector v col-name ValueVector)]
                  (->direct-vec child-vec)
                  (->NullIndirectVector))

                (instance? DenseUnionVector v)
                (let [^DenseUnionVector v v
                      vecs (mapv #(reader-for-key % col-name) (.getChildrenFromFields v))]
                  (reify IIndirectVector
                    (getName [_] col-name)

                    (isPresent [_ idx]
                      ;; TODO `(.getOffset v idx)` rather than just `idx`?
                      ;; haven't made a test fail with it yet, either way.
                      (.isPresent ^IIndirectVector (nth vecs (.getTypeId v idx)) idx))

                    (rowCopier [_ivec w]
                      (let [copiers (mapv #(.rowCopier ^IIndirectVector % w) vecs)]
                        (reify IRowCopier
                          (copyRow [_ idx]
                            (.copyRow ^IRowCopier (nth copiers (.getTypeId v idx))
                                      (.getOffset v idx))))))))

                :else (->NullIndirectVector)))]

      (reader-for-key v col-name))))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IAbstractListVector
  (^int getElementStartIndex [^int idx])
  (^int getElementEndIndex [^int idx])
  (^org.apache.arrow.vector.ValueVector getVector [])
  (^org.apache.arrow.vector.ValueVector getDataVector []))

(defn- ->list-reader [^ValueVector v]
  (letfn [(->list-reader* [^IAbstractListVector alv]
            (let [list-vec (.getVector alv)
                  data-vec (->direct-vec (.getDataVector alv))]
              (reify IListReader
                (isPresent [_ idx] (not (.isNull list-vec idx)))
                (getElementStartIndex [_ idx] (.getElementStartIndex alv idx))
                (getElementEndIndex [_ idx] (.getElementEndIndex alv idx))
                (elementCopier [_ w]
                  (let [copier (.rowCopier data-vec w)]
                    (reify IListElementCopier
                      (copyElement [_ idx n]
                        (let [copy-idx (+ (.getElementStartIndex alv idx) n)]
                          (if (or (neg? n) (>= copy-idx (.getElementEndIndex alv idx)))
                            (throw (IndexOutOfBoundsException.))
                            (.copyRow copier copy-idx))))))))))]
    (cond
      (instance? ListVector v)
      (let [^ListVector v v]
        (->list-reader* (reify IAbstractListVector
                          (getElementStartIndex [_ idx] (.getElementStartIndex v idx))
                          (getElementEndIndex [_ idx] (.getElementEndIndex v idx))
                          (getVector [_] v)
                          (getDataVector [_] (.getDataVector v)))))

      (instance? FixedSizeListVector v)
      (let [^FixedSizeListVector v v]
        (->list-reader* (reify IAbstractListVector
                          (getElementStartIndex [_ idx] (.getElementStartIndex v idx))
                          (getElementEndIndex [_ idx] (.getElementEndIndex v idx))
                          (getVector [_] v)
                          (getDataVector [_] (.getDataVector v)))))

      (instance? DenseUnionVector v)
      (let [^DenseUnionVector v v
            vecs (mapv ->list-reader (.getChildrenFromFields v))]
        (reify IListReader
          (isPresent [_ idx]
            (.isPresent ^IListReader (nth vecs (.getTypeId v idx)) (.getOffset v idx)))
          (getElementStartIndex [_ idx]
            (let [^IListReader vec (nth vecs (.getTypeId v idx))]
              (.getElementStartIndex vec idx)))
          (getElementEndIndex [_ idx]
            (let [^IListReader vec (nth vecs (.getTypeId v idx))]
              (.getElementEndIndex vec idx)))
          (elementCopier [this w]
            (let [copiers (mapv #(some-> ^IListReader % (.elementCopier w)) vecs)]
              (reify IListElementCopier
                (copyElement [_ idx n]
                  (let [^IListElementCopier copier (nth copiers (.getTypeId v idx))]
                    (when (.isPresent this idx)
                      (.copyElement copier (.getOffset v idx) n)))))))))

      :else
      (reify IListReader
        (isPresent [_ _idx] false)
        (getElementStartIndex [_ _idx] (throw (UnsupportedOperationException. "Not implemented")))
        (getElementEndIndex [_ _idx] (throw (UnsupportedOperationException. "Not implemented")))
        (elementCopier [_ _w]
          (reify IListElementCopier
            (copyElement [_ _idx _n]
              (throw (UnsupportedOperationException. "Not implemented")))))))))

(defrecord DirectVector [^ValueVector v, ^String name]
  IIndirectVector
  (isPresent [_ _] true)
  (getVector [_] v)
  (getIndex [_ idx] idx)
  (getName [_] name)
  (getValueCount [_] (.getValueCount v))

  (withName [_ name] (->DirectVector v name))
  (select [this idxs] (->IndirectVector this idxs))

  (copyTo [_ out-vec]
    ;; we'd like to use .getTransferPair here but DUV is broken again
    ;; - it doesn't pass the fieldType through so you get a DUV with empty typeIds
    (doto (.makeTransferPair v out-vec)
      (.splitAndTransfer 0 (.getValueCount v)))

    (DirectVector. out-vec name))

  (rowCopier [_ w] (.rowCopier w v))
  (structReader [_] (->StructReader v))
  (listReader [_] (->list-reader v))

  (monoReader [_ col-type] (vec/->mono-reader v col-type))
  (polyReader [_ col-type] (vec/->poly-reader v col-type)))

(defn compose-selection
  "Returns the composition of the selections sel1, sel2 which when applied to a vector will be the same as (select (select iv sel1) sel2).

  Use to avoid intermediate vector allocations.

  A selection looks like this: [3, 1, 2, 0] which when applied to a vector, will yield a new vector [vec[3], vec[1], vec[2], vec[0]].

  Selections are composed to form a new selection.

  composing [3, 1, 2, 0] and [2, 2, 0] => [1, 1, 3]

  See also: IIndirectVector .select"
  ^ints [^ints sel1 ^ints sel2]
  (let [new-left (int-array (alength sel2))]
    (dotimes [idx (alength sel2)]
      (aset new-left idx (aget sel1 (aget sel2 idx))))
    new-left))

(defn- copy-to! [^IIndirectVector col, ^ValueVector out-vec]
  (.clear out-vec)

  (let [in-vec (.getVector col)
        src-value-count (.getValueCount col)]
    (if (instance? DenseUnionVector in-vec)
      ;; DUV.copyValueSafe is broken - it's not safe, and it calls DenseUnionWriter.setPosition which NPEs
      (let [^DenseUnionVector from-duv in-vec
            ^DenseUnionVector to-duv out-vec]
        (dotimes [idx src-value-count]
          (let [src-idx (.getIndex col idx)
                type-id (.getTypeId from-duv src-idx)
                dest-sub-vec (.getVectorByType to-duv type-id)
                dest-offset (.getValueCount dest-sub-vec)
                tp (.makeTransferPair (.getVectorByType from-duv type-id) dest-sub-vec)]
            (.setTypeId to-duv idx type-id)
            (.setOffset to-duv idx dest-offset)
            (.copyValueSafe tp (.getOffset from-duv src-idx) dest-offset)
            (.setValueCount to-duv (inc idx)))))
      (let [tp (.makeTransferPair in-vec out-vec)]
        (dotimes [idx src-value-count]
          (.copyValueSafe tp (.getIndex col idx) idx)))))

  (DirectVector. (doto out-vec
                   (.setValueCount (.getValueCount col)))
                 (.getName col)))

(defrecord IndirectVector [^IIndirectVector v, ^ints idxs]
  IIndirectVector
  (getVector [_] (.getVector v))
  (getIndex [_ idx] (aget idxs idx))
  (getName [_] (.getName v))
  (getValueCount [_] (alength idxs))

  (withName [_ col-name] (IndirectVector. (.withName v col-name) idxs))

  (isPresent [_ idx] (.isPresent v (aget idxs idx)))

  (select [this new-idxs]
    (IndirectVector. v (compose-selection (.idxs this) new-idxs)))

  (copyTo [this out-vec] (copy-to! this out-vec))

  (rowCopier [this-vec w]
    (let [copier (.rowCopier v w)]
      (reify IRowCopier
        (copyRow [_ idx]
          (.copyRow copier (.getIndex this-vec idx))))))

  (monoReader [this col-type]
    (-> (.monoReader v col-type)
        (vec/->IndirectVectorMonoReader this)))

  (polyReader [this col-type]
    (-> (.polyReader v col-type)
        (vec/->IndirectVectorPolyReader this))))

(defn ->direct-vec ^core2.vector.IIndirectVector [^ValueVector in-vec]
  (DirectVector. in-vec (.getName in-vec)))

(defn ->indirect-vec ^core2.vector.IIndirectVector [^ValueVector in-vec, ^ints idxs]
  (IndirectVector. (->direct-vec in-vec) idxs))

(deftype IndirectRelation [^Map cols, ^long row-count]
  IIndirectRelation
  (vectorForName [_ col-name] (.get cols col-name))
  (rowCount [_] row-count)

  (iterator [_] (.iterator (.values cols)))

  (close [_] (run! util/try-close (.values cols))))

(defn ->indirect-rel
  (^core2.vector.IIndirectRelation [cols]
   (->indirect-rel cols
                   (if-let [^IIndirectVector col (first cols)]
                     (.getValueCount col)
                     0)))

  (^core2.vector.IIndirectRelation [cols ^long row-count]
   (IndirectRelation. (let [col-map (LinkedHashMap.)]
                        (doseq [^IIndirectVector col cols]
                          (.put col-map (.getName col) col))
                        col-map)
                      row-count)))

(defn <-root ^core2.vector.IIndirectRelation [^VectorSchemaRoot root]
  (let [cols (LinkedHashMap.)]
    (doseq [^ValueVector in-vec (.getFieldVectors root)]
      (.put cols (.getName in-vec) (->direct-vec in-vec)))
    (IndirectRelation. cols (.getRowCount root))))

(defn select ^core2.vector.IIndirectRelation [^IIndirectRelation in-rel, ^ints idxs]
  (->indirect-rel (for [^IIndirectVector in-col in-rel]
                    (.select in-col idxs))
                  (alength idxs)))

(defn copy ^core2.vector.IIndirectRelation [^IIndirectRelation in-rel, ^BufferAllocator allocator]
  (->indirect-rel (for [^IIndirectVector in-col in-rel]
                    (.copy in-col allocator))
                  (.rowCount in-rel)))

(deftype SliceVector [^IIndirectVector col, ^long start-idx, ^long len]
  IIndirectVector
  (getVector [_] (.getVector col))
  (getIndex [_ idx] (+ start-idx idx))
  (getName [_] (.getName col))
  (getValueCount [_] len)
  (withName [_ col-name] (SliceVector. (.withName col col-name) start-idx len))
  (isPresent [this idx] (.isPresent col (.getIndex this idx)))
  (select [this new-idxs] (IndirectVector. this new-idxs))
  (copyTo [this out-vec] (copy-to! this out-vec))

  (rowCopier [this-vec w]
    (let [copier (.rowCopier col w)]
      (reify IRowCopier
        (copyRow [_ idx]
          (.copyRow copier (.getIndex this-vec idx))))))

  (monoReader [this col-type]
    (-> (.monoReader col col-type)
        (vec/->IndirectVectorMonoReader this)))

  (polyReader [this col-type]
    (-> (.polyReader col col-type)
        (vec/->IndirectVectorPolyReader this))))

(defn slice-col ^core2.vector.IIndirectVector [^IIndirectVector col, ^long start-idx, ^long len]
  (SliceVector. col start-idx len))

(defn rel->rows ^java.lang.Iterable [^IIndirectRelation rel]
  (let [ks (for [^IIndirectVector col rel]
             (keyword (.getName col)))]
    (mapv (fn [idx]
            (zipmap ks
                    (for [^IIndirectVector col rel]
                      (let [v (.getVector col)
                            i (.getIndex col idx)]
                        (when-not (.isNull v i)
                          (ty/get-object v i))))))
          (range (.rowCount rel)))))

(deftype SliceCursor [^IIndirectRelation rel
                      ^:volatile-mutable ^long start-idx
                      ^Iterator row-counts]
  ICursor
  (tryAdvance [this c]
    (if (.hasNext row-counts)
      (let [^long row-count (.next row-counts)]
        (.accept c (->indirect-rel (for [col rel]
                                     (slice-col col start-idx row-count))))
        (set! (.-start-idx this) (+ (.-start-idx this) row-count))
        true)
      false))

  (close [_]
    ;; we don't close the rel here, assume we don't own it.
    ))

(defn ->slice-cursor [^IIndirectRelation rel, ^Iterable row-counts]
  (SliceCursor. rel 0 (.iterator row-counts)))

(deftype DuvChildReader [^IIndirectVector parent-col
                         ^DenseUnionVector parent-duv
                         ^byte type-id
                         ^ValueVector type-vec]
  IIndirectVector
  (getVector [_] type-vec)
  (getIndex [_ idx] (.getOffset parent-duv (.getIndex parent-col idx)))
  (getName [_] (.getName parent-col))
  (withName [_ name] (DuvChildReader. (.withName parent-col name) parent-duv type-id type-vec)))

(defn duv-type-id ^java.lang.Byte [^DenseUnionVector duv, col-type]
  (let [field (.getField duv)
        type-ids (.getTypeIds ^ArrowType$Union (.getType field))
        duv-leg-key (ty/col-type->duv-leg-key col-type)]
    (-> (keep-indexed (fn [idx ^Field sub-field]
                        (when (= duv-leg-key (-> (ty/field->col-type sub-field)
                                                 (ty/col-type->duv-leg-key)))
                          (aget type-ids idx)))
                      (.getChildren field))
        (first))))
