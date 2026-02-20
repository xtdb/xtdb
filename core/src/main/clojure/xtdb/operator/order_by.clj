(ns xtdb.operator.order-by
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [xtdb.expression.comparator :as expr.comp]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import (java.io File OutputStream)
           (java.nio.channels Channels)
           java.nio.file.Path
           (java.util HashMap List Map PriorityQueue)
           (java.util Comparator)
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.arrow ArrowUnloader$Mode Relation Relation$Loader RelationReader RowCopier Vector)
           xtdb.ICursor))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(s/def ::order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                           :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                    (s/nonconforming))
                                :kind vector?))

(defmethod lp/ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :opts (s/keys :req-un [::order-specs])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *block-size* (int 102400))

(defn- ->file [tmp-dir batch-idx file-idx]
  (io/file tmp-dir (format "temp-sort-%d-%d.arrow" batch-idx file-idx)))

(defn- write-rel [^BufferAllocator allocator, ^Relation rel, ^OutputStream os]
  (util/with-open [ch (Channels/newChannel os)
                   rel (.openDirectSlice rel allocator)
                   unl (.startUnload rel ch ArrowUnloader$Mode/FILE)]
    (.writePage unl)
    (.end unl)))

(defn sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (-> (IntStream/range 0 (.getRowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (.vectorForOrNull read-rel (str column))
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
    (if-let [filename (with-open [out-rel (Relation. allocator)]
                        (while (and (<= (.getRowCount out-rel) ^int *block-size*)
                                    (.tryAdvance in-cursor
                                                 (fn [^RelationReader src-rel]
                                                   (.append out-rel src-rel)))))
                        (when (pos? (.getRowCount out-rel))
                          (let [out-filename (->file tmp-dir 0 file-idx)]
                            (with-open [os (io/output-stream out-filename)]
                              (write-rel allocator (.select out-rel (sorted-idxs out-rel order-specs)) os)
                              out-filename))))]
      (recur (conj filenames filename) (inc file-idx))
      (mapv io/file filenames))))

(defn mk-rel-comparator [^Relation rel1, ^Relation rel2, order-specs]
  (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                         :or {direction :asc, null-ordering :nulls-last}}]]
            (let [read-col1 (.get rel1 (str column))
                  read-col2 (.get rel2 (str column))
                  col-comparator (expr.comp/->comparator read-col1 read-col2 null-ordering)

                  ^Comparator
                  comparator (if (= :desc direction)
                               (fn [left right]
                                 (- (.applyAsInt col-comparator left right)))
                               (fn [left right]
                                 (.applyAsInt col-comparator left right)))]
              (if acc
                (.thenComparing acc comparator)
                comparator)))
          nil
          order-specs))

(defn k-way-merge [^BufferAllocator allocator filenames order-specs vec-types tmp-dir batch-idx file-idx]
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
        (with-open [out-rel (Relation. allocator ^Map (update-keys vec-types str))
                    out-unl (.startUnload out-rel (Channels/newChannel (io/output-stream out-file)))]
          (dotimes [i k]
            (aset copiers i (.rowCopier ^Relation (nth rels i) out-rel)))

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

(defn- external-sort [allocator in-cursor order-specs vec-types tmp-dir first-filename]
  (let [batches (write-out-rels allocator in-cursor order-specs tmp-dir first-filename)]
    (loop [batches batches
           batch-idx 1]
      (if-not (< 1 (count batches))
        (first batches)
        (recur
         (loop [batches batches new-batches [] file-idx 0]
           (if-let [files (seq (take k-way-constant batches))]
             (let [new-batch (try
                               (k-way-merge allocator files order-specs vec-types tmp-dir batch-idx file-idx)
                               (finally
                                 (run! io/delete-file files)))]

               (recur (drop k-way-constant batches)
                      (conj new-batches new-batch)
                      (inc file-idx)))
             new-batches))
         (inc batch-idx))))))

(deftype OrderByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        static-vec-types
                        order-specs
                        ^:unsynchronized-mutable ^boolean consumed?
                        ^:unsynchronized-mutable ^Path sort-dir
                        ^:unsynchronized-mutable ^Relation$Loader loader
                        ^:unsynchronized-mutable ^Relation read-rel
                        ^:unsynchronized-mutable ^File sorted-file]
  ICursor
  (getCursorType [_] "order-by")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (if-not consumed?
      (letfn [(load-next-batch []
                (if (.loadNextPage ^Relation$Loader (.loader this) (.read-rel this))
                  (do
                    (.accept c (.read-rel this))
                    true)
                  (do
                    (io/delete-file (.sorted-file this))
                    (set! (.consumed? this) true)
                    false)))]

        (if-not (nil? loader)
          (load-next-batch)

          (util/with-open [acc-rel (Relation. allocator
                                              ^List (vec (for [[col-name vec-type] static-vec-types]
                                                           (Vector/open allocator (str col-name) vec-type)))
                                              0)]
            (while (and (<= (.getRowCount acc-rel) ^int *block-size*)
                        (.tryAdvance in-cursor
                                     (fn [^RelationReader src-rel]
                                       (.append acc-rel src-rel)))))

            (let [pos (.getRowCount acc-rel)]
              (if (<= pos ^int *block-size*)
                ;; in memory case
                (if (zero? pos)
                  (do
                    (set! (.consumed? this) true)
                    false)

                  (do
                    (.accept c (.select acc-rel (sorted-idxs acc-rel order-specs)))
                    (set! (.consumed? this) true)
                    true))

                ;; first batch from external sort
                (let [sort-dir (util/tmp-dir "external-sort")
                      tmp-dir (io/file (.getPath (.toUri sort-dir)))
                      first-filename (->file tmp-dir 0 0)]
                  (set! (.sort-dir this) sort-dir)
                  (.mkdirs tmp-dir)
                  (let [out-rel (.select acc-rel (sorted-idxs acc-rel order-specs))]
                    (with-open [os (io/output-stream first-filename)]
                      (write-rel allocator out-rel os)))

                  (let [sorted-file (external-sort allocator in-cursor order-specs static-vec-types tmp-dir first-filename)]
                    (set! (.sorted-file this) sorted-file)
                    (let [loader (Relation/loader allocator (util/->file-channel (util/->path sorted-file)))]
                      (set! (.loader this) loader)
                      (set! (.read-rel this) (Relation. allocator (.getSchema loader))))
                    (load-next-batch))))))))
      false))

  (close [_]
    (util/try-close read-rel)
    (util/try-close loader)
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :order-by [{:keys [opts relation]} args]
  (let [{:keys [order-specs]} opts]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [{:keys [vec-types], :as rel}]
        {:op :order-by
         :children [rel]
         :explain {:order-specs (pr-str order-specs)}
         :vec-types vec-types
         :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                     (cond-> (OrderByCursor. allocator in-cursor vec-types order-specs false nil nil nil nil)
                       (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))))
