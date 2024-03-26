(ns xtdb.operator.order-by
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [xtdb.expression.comparator :as expr.comp]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IPersistentMap PersistentVector)
           (java.io File OutputStream)
           (java.nio.channels Channels)
           java.nio.file.Path
           (java.util HashMap PriorityQueue)
           (java.util Comparator)
           (java.util.function Consumer ToIntFunction)
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector ValueVector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter)
           org.apache.arrow.vector.types.pojo.Field
           xtdb.ICursor
           (xtdb.vector IRowCopier IVectorPosition IVectorReader RelationReader RelationWriter)))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(defmethod lp/ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                            :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                     (s/nonconforming))
                                 :kind vector?)
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *chunk-size* (int 102400))

(defn- ->file [tmp-dir batch-idx file-idx]
  (io/file tmp-dir (format "temp-sort-%d-%d.arrow" batch-idx file-idx)))

(defn- write-rel [^BufferAllocator allocator, ^RelationReader rdr, ^OutputStream os]
  (let [new-vecs (for [^IVectorReader col rdr]
                   (let [^ValueVector new-vec (.createVector (-> (.getField col)
                                                                 (types/field-with-name (.getName col)))
                                                             allocator)]

                     (.copyTo col new-vec)
                     new-vec))]
    (try
      (with-open [root (VectorSchemaRoot. ^PersistentVector (vec new-vecs))]
        (let [writer (ArrowStreamWriter. root nil (Channels/newChannel os))]
          (.start writer)
          (.writeBatch writer)
          (.end writer)))
      (catch Throwable e
        (run! util/close new-vecs)
        (throw e)))))

(defn- sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (.readerForName read-rel (name column))
                               col-comparator (expr.comp/->comparator read-col read-col null-ordering)

                               ^Comparator
                               comparator (cond-> (reify Comparator
                                                    (compare [_ left right]
                                                      (.applyAsInt col-comparator left right)))
                                            (= :desc direction) (.reversed))]
                           (if acc
                             (.thenComparing acc comparator)
                             comparator)))
                       nil
                       order-specs))
      (.mapToInt (reify ToIntFunction
                   (applyAsInt [_ x] x)))
      (.toArray)))

(defn- write-out-rels [allocator ^ICursor in-cursor, order-specs tmp-dir first-filename]
  (loop [filenames [first-filename] file-idx 1] ;; file-idx 1 as first-filename contains 0
    (if-let [filename (with-open [rel-writer (vw/->rel-writer allocator)]
                        (let [pos (.writerPosition rel-writer)]
                          (while (and (<= (.getPosition pos) ^int *chunk-size*)
                                      (.tryAdvance in-cursor
                                                   (reify Consumer
                                                     (accept [_ src-rel]
                                                       (vw/append-rel rel-writer src-rel))))))
                          (when (pos? (.getPosition pos))
                            (let [read-rel (vw/rel-wtr->rdr rel-writer)
                                  out-filename (->file tmp-dir 0 file-idx)]
                              (with-open [out-rel (.select read-rel (sorted-idxs read-rel order-specs))

                                          os (io/output-stream out-filename)]
                                (write-rel allocator out-rel os)
                                out-filename)))))]
      (recur (conj filenames filename) (inc file-idx))
      (mapv io/file filenames))))

(defn mk-irel-comparator [^RelationReader irel1 ^RelationReader irel2 order-specs]
  (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                         :or {direction :asc, null-ordering :nulls-last}}]]
            (let [read-col1 (.readerForName irel1 (name column))
                  read-col2 (.readerForName irel2 (name column))
                  col-comparator (expr.comp/->comparator read-col1 read-col2 null-ordering)

                  ^Comparator
                  comparator (cond-> (reify Comparator
                                       (compare [_ left right]
                                         (.applyAsInt col-comparator left right)))
                               (= :desc direction) (.reversed))]
              (if acc
                (.thenComparing acc comparator)
                comparator)))
          nil
          order-specs))

(defn- fields->vsr ^VectorSchemaRoot [^BufferAllocator allocator fields]
  (VectorSchemaRoot. ^PersistentVector (vec (for [[col-name field] fields]
                                              (.createVector (types/field-with-name field (str col-name))
                                                             allocator)))))

(defn k-way-merge [^BufferAllocator allocator filenames order-specs fields tmp-dir batch-idx file-idx]
  (let [k (count filenames)
        out-file (->file tmp-dir batch-idx file-idx)
        rdrs (object-array (mapv #(ArrowStreamReader. (io/input-stream %) allocator) filenames))
        vsrs (object-array (mapv #(.getVectorSchemaRoot ^ArrowStreamReader %) rdrs))
        rel-rdrs (object-array k)
        copiers (object-array k)
        positions (object-array (repeatedly k #(IVectorPosition/build)))]
    (try
      (with-open [root-out (fields->vsr allocator fields)
                  wrt (ArrowStreamWriter. root-out nil (io/output-stream out-file))
                  rel-wrt (vw/root->writer root-out)]
        (let [rel-wrt-cnt (IVectorPosition/build)]
          (letfn [(load-next-rel [i]
                    (when (aget rel-rdrs i)
                      (util/close (aget rel-rdrs i)))
                    (when (.loadNextBatch ^ArrowStreamReader (aget rdrs i))
                      (aset rel-rdrs i (vr/<-root (aget vsrs i)))
                      (aset copiers i (.rowCopier rel-wrt (aget rel-rdrs i)))
                      (.setPosition ^IVectorPosition (aget positions i) 0)
                      true))]
            (.start wrt)
            (let [cmps (HashMap.)
                  pq (PriorityQueue. (fn [^long i ^long j]
                                       (if (< i j)
                                         (.compare ^java.util.Comparator
                                                   (.get cmps [i j])
                                                   (.getPosition ^IVectorPosition (aget positions i))
                                                   (.getPosition ^IVectorPosition (aget positions j)))
                                         (* -1 (.compare ^java.util.Comparator
                                                         (.get cmps [j i])
                                                         (.getPosition ^IVectorPosition (aget positions j))
                                                         (.getPosition ^IVectorPosition (aget positions i)))))))]
              (doseq [i (range k)]
                (load-next-rel i))

              (doseq [i (range k)
                      j (range i)]
                (.put cmps [j i] (mk-irel-comparator (aget rel-rdrs j) (aget rel-rdrs i) order-specs)))

              (doseq [i (range k)]
                (when-let [^RelationReader rel-rdr (aget rel-rdrs i)]
                  (when (pos-int? (.rowCount rel-rdr))
                    (.add pq i))))

              (while (not (.isEmpty pq))
                (let [^long i (.poll pq)
                      ^IVectorPosition position (aget positions i)]
                  (.copyRow ^IRowCopier (aget copiers i) (.getPositionAndIncrement position))
                  (if (< (.getPosition position) (.rowCount ^RelationReader (aget rel-rdrs i)))
                    (.add pq i)
                    (when (load-next-rel i)
                      ;; TODO can this extra comparator creation be avoided?
                      (let [new-rdr (aget rel-rdrs i)]
                        (doseq [^long j (range k)
                                :when (not= i j)]
                          (if (< i j)
                            (.put cmps [i j] (mk-irel-comparator new-rdr (aget rel-rdrs j) order-specs))
                            (.put cmps [j i] (mk-irel-comparator (aget rel-rdrs j) new-rdr order-specs)))))
                      (.add pq i)))
                  ;; spill next chunk
                  (when (< ^int *chunk-size* (.getPositionAndIncrement rel-wrt-cnt))
                    (.syncRowCount rel-wrt)
                    (.writeBatch wrt)
                    (.clear rel-wrt)
                    (.setPosition rel-wrt-cnt 0)
                    (doseq [i (range k)]
                      (aset copiers i (.rowCopier rel-wrt (aget rel-rdrs i))))))))
            ;; spill remaining rows
            (when (pos-int? (.getPosition rel-wrt-cnt))
              (.syncRowCount rel-wrt)
              (.writeBatch wrt))
            (.end wrt)
            out-file)))
      (finally
        (run! util/close rdrs)
        (run! util/close vsrs)))))

(def ^:private k-way-constant 4)

(defn- external-sort [allocator ^ICursor in-cursor, order-specs, fields, tmp-dir, first-filename]
  (let [batches (write-out-rels allocator in-cursor order-specs tmp-dir first-filename)]
    (loop [batches batches batch-idx 1]
      (if-not (< 1 (count batches))
        (first batches)
        (recur
         (loop [batches batches new-batches [] file-idx 0]
           (if-let [files (seq (take k-way-constant batches))]
             (let [new-batch (k-way-merge allocator files order-specs fields tmp-dir batch-idx file-idx)]
               (dorun (map io/delete-file files))
               (recur (drop k-way-constant batches)
                      (conj new-batches new-batch)
                      (inc file-idx)))
             new-batches))
         (inc batch-idx))))))

(deftype OrderByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^IPersistentMap static-fields
                        order-specs
                        ^:unsynchronized-mutable ^boolean consumed?
                        ^:unsynchronized-mutable ^Path sort-dir
                        ^:unsynchronized-mutable ^ArrowStreamReader reader
                        ^:unsynchronized-mutable ^VectorSchemaRoot read-root
                        ^:unsynchronized-mutable ^File sorted-file]
  ICursor
  (tryAdvance [this c]
    (if-not consumed?
      (letfn [(load-next-batch []
                (if (.loadNextBatch ^ArrowStreamReader (.reader this))
                  (with-open [out-rel (vr/<-root (.read-root this))]
                    (.accept c out-rel)
                    true)
                  (do
                    (io/delete-file (.sorted-file this))
                    (set! (.consumed? this) true)
                    false)))]
        (if-not (nil? reader)
          (load-next-batch)
          (with-open [rel-writer (RelationWriter. allocator (for [[col-name ^Field field] static-fields]
                                                              (-> (types/field-with-name field (str col-name))
                                                                  (.createVector allocator)
                                                                  vw/->writer)))]
            (let [pos (.writerPosition rel-writer)]
              (while (and (<= (.getPosition pos) ^int *chunk-size*)
                          (.tryAdvance in-cursor
                                       (reify Consumer
                                         (accept [_ src-rel]
                                           (vw/append-rel rel-writer src-rel))))))
              (let [read-rel (vw/rel-wtr->rdr rel-writer)]
                (if (<= (.getPosition pos) ^int *chunk-size*)
                  ;; in memory case
                  (if (zero? (.getPosition pos))
                    (do (set! (.consumed? this) true)
                        false)
                    (do (.accept c (.select read-rel (sorted-idxs read-rel order-specs)))
                        (set! (.consumed? this) true)
                        true))

                  ;; first batch from external sort
                  (let [sort-dir (util/tmp-dir "external-sort")
                        ^java.io.File tmp-dir (io/file (.getPath (.toUri sort-dir)))
                        first-filename (->file tmp-dir 0 0)]
                    (set! (.sort-dir this) sort-dir)
                    (.mkdirs tmp-dir)
                    (with-open [out-rel (.select read-rel (sorted-idxs read-rel order-specs))
                                os (io/output-stream first-filename)]
                      (write-rel allocator out-rel os))

                    (let [sorted-file (external-sort allocator in-cursor order-specs static-fields tmp-dir first-filename)]
                      (set! (.sorted-file this) sorted-file)
                      (set! (.reader this) (ArrowStreamReader. (io/input-stream sorted-file) allocator))
                      (set! (.read-root this) (.getVectorSchemaRoot reader))
                      (load-next-batch)))))))
          ))
      false))

  (close [_]
    (util/try-close read-root)
    (util/try-close reader)
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :order-by [{:keys [order-specs relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [fields]
      {:fields fields
       :->cursor (fn [{:keys [allocator]} in-cursor]
                   (OrderByCursor. allocator in-cursor fields order-specs false nil nil nil nil))})))
