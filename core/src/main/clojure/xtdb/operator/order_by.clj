(ns xtdb.operator.order-by
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [xtdb.expression.comparator :as expr.comp]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IPersistentMap)
           (java.io File OutputStream)
           (java.nio.channels Channels)
           java.nio.file.Path
           (java.util HashMap List PriorityQueue)
           (java.util Comparator)
           (java.util.function Consumer)
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowFileReader ArrowReader ArrowFileWriter)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.arrow Relation Relation$Loader RowCopier VectorReader)
           xtdb.ICursor
           (xtdb.vector IVectorReader RelationReader RelationWriter)))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(s/def ::order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                           :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                    (s/nonconforming))
                                :kind vector?))

(defmethod lp/ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :order-specs ::order-specs
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *block-size* (int 102400))

(defn- ->file [tmp-dir batch-idx file-idx]
  (io/file tmp-dir (format "temp-sort-%d-%d.arrow" batch-idx file-idx)))

(defn- write-rel [^BufferAllocator allocator, ^RelationReader rdr, ^OutputStream os]
  (util/with-close-on-catch [new-vecs (vec (for [^IVectorReader col rdr]
                                             (let [new-vec (.createVector (-> (.getField col)
                                                                              (types/field-with-name (.getName col)))
                                                                          allocator)]
                                               (.copyTo col new-vec)
                                               new-vec)))]
    (with-open [root (VectorSchemaRoot. ^List new-vecs)]
      (doto (ArrowFileWriter. root nil (Channels/newChannel os))
        (.start) (.writeBatch) (.end)))))

(defn sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (VectorReader/from (.readerForName read-rel (str column)))
                               col-comparator (expr.comp/->comparator read-col read-col null-ordering)

                               comparator (cond-> ^Comparator (fn [left right]
                                                                (.applyAsInt col-comparator left right))
                                            (= :desc direction) (.reversed))]
                           (if acc
                             (.thenComparing acc comparator)
                             comparator)))
                       nil
                       order-specs))
      (.mapToInt identity)
      (.toArray)))

(defn- write-out-rels [allocator ^ICursor in-cursor, order-specs tmp-dir first-filename]
  (loop [filenames [first-filename]
         ;; file-idx 1 as first-filename contains 0
         file-idx 1]
    (if-let [filename (with-open [rel-writer (vw/->rel-writer allocator)]
                        (let [pos (.writerPosition rel-writer)]
                          (while (and (<= (.getPosition pos) ^int *block-size*)
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

(defn mk-rel-comparator [^Relation rel1, ^Relation rel2, order-specs]
  (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                         :or {direction :asc, null-ordering :nulls-last}}]]
            (let [read-col1 (.get rel1 (str column))
                  read-col2 (.get rel2 (str column))
                  col-comparator (expr.comp/->comparator read-col1 read-col2 null-ordering)

                  ^Comparator
                  comparator (cond-> ^Comparator (fn [left right]
                                                   (.applyAsInt col-comparator left right))
                               (= :desc direction) (.reversed))]
              (if acc
                (.thenComparing acc comparator)
                comparator)))
          nil
          order-specs))

(defn rename-fields [fields]
  (vec (for [[col-name field] fields]
         (types/field-with-name field (str col-name)))))

(defn k-way-merge [^BufferAllocator allocator filenames order-specs ^List fields tmp-dir batch-idx file-idx]
  (let [k (count filenames)
        out-file (->file tmp-dir batch-idx file-idx)]

    (util/with-open [loaders (mapv (fn [file-name]
                                     (try
                                       (Relation/loader allocator (util/->file-channel (util/->path file-name)))
                                       (catch Exception t
                                         (throw (ex-info "unable to read file" {:file-name file-name} t)))))
                                   filenames)
                     rels (mapv #(Relation. allocator (.getSchema ^Relation$Loader %)) loaders)]

      (let [copiers (object-array k)
            positions (int-array k)]
        (with-open [out-rel (Relation. allocator fields)
                    out-unl (.startUnload out-rel (Channels/newChannel (io/output-stream out-file)))]
          (dotimes [i k]
            (aset copiers i (.rowCopier out-rel ^Relation (nth rels i))))

          (letfn [(load-next-rel [i]
                    (when (.loadNextPage ^Relation$Loader (nth loaders i)
                                         ^Relation (nth rels i))
                      (aset positions i 0)
                      true))]
            (let [cmps (HashMap.)
                  pq (PriorityQueue. (fn [^long i ^long j]
                                       (if (< i j)
                                         (.compare ^java.util.Comparator (.get cmps [i j])
                                                   (aget positions i)
                                                   (aget positions j))
                                         (- (.compare ^java.util.Comparator (.get cmps [j i])
                                                      (aget positions j)
                                                      (aget positions i))))))]
              (dotimes [i k]
                (load-next-rel i))

              (doseq [i (range k)
                      j (range i)]
                (.put cmps [j i] (mk-rel-comparator (nth rels j) (nth rels i) order-specs)))

              (dotimes [i k]
                (when-let [^Relation rel (nth rels i)]
                  (when (pos? (.getRowCount rel))
                    (.add pq i))))

              (while (not (.isEmpty pq))
                (let [^long i (.poll pq)
                      pos (aget positions i)]
                  (aset positions i (inc pos))
                  (.copyRow ^RowCopier (aget copiers i) pos)

                  (if (< (inc pos) (.getRowCount ^Relation (nth rels i)))
                    (.add pq i)
                    (when (load-next-rel i)
                      (.add pq i)))

                  ;; spill next block
                  (when (< ^int *block-size* (.getRowCount out-rel))
                    (.writePage out-unl)
                    (.clear out-rel)))))

            ;; spill remaining rows
            (when (pos? (.getRowCount out-rel))
              (.writePage out-unl))
            (.end out-unl)
            out-file))))))

(def ^:private k-way-constant 4)

(defn- external-sort [allocator in-cursor order-specs fields tmp-dir first-filename]
  (let [batches (write-out-rels allocator in-cursor order-specs tmp-dir first-filename)]
    (loop [batches batches
           batch-idx 1]
      (if-not (< 1 (count batches))
        (first batches)
        (recur
         (loop [batches batches new-batches [] file-idx 0]
           (if-let [files (seq (take k-way-constant batches))]
             (let [new-batch (try
                               (k-way-merge allocator files order-specs fields tmp-dir batch-idx file-idx)
                               (finally
                                 #_(run! io/delete-file files)))]

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
                        ^:unsynchronized-mutable ^ArrowReader reader
                        ^:unsynchronized-mutable ^VectorSchemaRoot read-root
                        ^:unsynchronized-mutable ^File sorted-file]
  ICursor
  (tryAdvance [this c]
    (if-not consumed?
      (letfn [(load-next-batch []
                (if (.loadNextBatch ^ArrowReader (.reader this))
                  (with-open [out-rel (vr/<-root (.read-root this))]
                    (.accept c out-rel)
                    true)
                  (do
                    (io/delete-file (.sorted-file this))
                    (set! (.consumed? this) true)
                    false)))]
        (if-not (nil? reader)
          (load-next-batch)
          (with-open [rel-writer (RelationWriter. allocator (for [^Field field static-fields]
                                                              (vw/->writer (.createVector field allocator))))]
            (let [pos (.writerPosition rel-writer)]
              (while (and (<= (.getPosition pos) ^int *block-size*)
                          (.tryAdvance in-cursor
                                       (reify Consumer
                                         (accept [_ src-rel]
                                           (vw/append-rel rel-writer src-rel))))))
              (let [read-rel (vw/rel-wtr->rdr rel-writer)]
                (if (<= (.getPosition pos) ^int *block-size*)
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
                      (set! (.reader this) (ArrowFileReader. (util/->file-channel (util/->path sorted-file)) allocator))
                      (set! (.read-root this) (.getVectorSchemaRoot reader))
                      (load-next-batch)))))))))
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
                   (OrderByCursor. allocator in-cursor (rename-fields fields) order-specs false nil nil nil nil))})))
