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
           (java.util Arrays Comparator)
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.arrow ArrowUnloader$Mode Relation Relation$Loader RelationReader RowCopier Vector)
           xtdb.api.ICursor))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(s/def ::order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                           :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                    (s/nonconforming))
                                :kind vector?))

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

;; :desc flips null-ordering before negating, not after - negating the
;; null outcomes too would move NULLS FIRST/LAST with direction, when
;; it's meant to be a fixed position (#6172).
(defn- ->order-comparator ^Comparator [read-col1 read-col2 direction null-ordering]
  (let [desc? (= :desc direction)
        col-comparator (expr.comp/->comparator read-col1 read-col2
                                                (if desc?
                                                  (case null-ordering
                                                    :nulls-first :nulls-last
                                                    :nulls-last :nulls-first)
                                                  null-ordering))]
    (if desc?
      (fn [left right] (- (.applyAsInt col-comparator left right)))
      (fn [left right] (.applyAsInt col-comparator left right)))))

(defn sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (-> (IntStream/range 0 (.getRowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (.vectorForOrNull read-rel (str column))
                               comparator (->order-comparator read-col read-col direction null-ordering)]
                           (if acc
                             (.thenComparing acc comparator)
                             comparator)))
                       nil
                       order-specs))
      (.mapToInt identity)
      (.toArray)))

(defn- top-idxs ^ints [^RelationReader read-rel, order-specs, ^long row-limit]
  (let [idxs (sorted-idxs read-rel order-specs)]
    (if (< row-limit (alength idxs))
      (Arrays/copyOf idxs (int row-limit))
      idxs)))

(defn- write-run ^File [^BufferAllocator allocator, ^Relation acc-rel, order-specs, row-limit, ^Path sort-dir, run-idx]
  (let [run (->file (.toFile sort-dir) 0 run-idx)]
    (with-open [os (io/output-stream run)]
      (write-rel allocator (.select acc-rel (top-idxs acc-rel order-specs row-limit)) os))
    run))

(defn mk-rel-comparator [^Relation rel1, ^Relation rel2, order-specs]
  (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                         :or {direction :asc, null-ordering :nulls-last}}]]
            (let [read-col1 (.get rel1 (str column))
                  read-col2 (.get rel2 (str column))
                  comparator (->order-comparator read-col1 read-col2 direction null-ordering)]
              (if acc
                (.thenComparing acc comparator)
                comparator)))
          nil
          order-specs))

(defn k-way-merge [^BufferAllocator allocator filenames order-specs row-limit vec-types tmp-dir batch-idx file-idx]
  (let [k (count filenames)
        row-limit (long row-limit)
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

              (loop [copied 0]
                (when (and (< copied row-limit) (not (.isEmpty pq)))
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
                      (.clear out-rel)))
                  (recur (inc copied)))))

            ;; spill remaining rows
            (when (pos? (.getRowCount out-rel))
              (.writePage out-unl))
            (.end out-unl)
            out-file))))))

(def ^:private k-way-constant 4)

(defn- merge-runs ^File [allocator runs order-specs row-limit vec-types tmp-dir]
  (loop [runs runs
         batch-idx 1]
    (if-not (< 1 (count runs))
      (first runs)
      (recur
       (loop [runs runs new-runs [] file-idx 0]
         (if-let [files (seq (take k-way-constant runs))]
           (let [new-run (try
                           (k-way-merge allocator files order-specs row-limit vec-types tmp-dir batch-idx file-idx)
                           (finally
                             (run! io/delete-file files)))]

             (recur (drop k-way-constant runs)
                    (conj new-runs new-run)
                    (inc file-idx)))
           new-runs))
       (inc batch-idx)))))

;; `append` copies each page in; call `finish` once, after the last `append`.
;; The cursor `finish` returns is valid until the sorter is closed; each page it hands out only until its next `tryAdvance`.
;; Closing the sorter deletes any spill files, whether or not `finish` was reached.
(gen-interface
 :name xtdb.operator.order_by.IExternalSorter
 :extends [java.lang.AutoCloseable]
 :methods [[append [xtdb.arrow.RelationReader] void]
           [finish [] xtdb.api.ICursor]])

(import xtdb.operator.order_by.IExternalSorter)

(deftype SpilledCursor [^Relation$Loader loader, ^Relation rel]
  ICursor
  (getCursorType [_] "spilled-sort")
  (getChildCursors [_] [])

  (tryAdvance [_ c]
    (if (.loadNextPage loader rel)
      (do
        (.accept c rel)
        true)
      false))

  (close [_]
    (util/try-close rel)
    (util/try-close loader)))

(deftype InMemoryCursor [^:unsynchronized-mutable ^RelationReader rel]
  ICursor
  (getCursorType [_] "in-memory-sort")
  (getChildCursors [_] [])

  (tryAdvance [_ c]
    (if-let [out-rel rel]
      (do
        (set! rel nil)
        (.accept c out-rel)
        true)
      false)))

(deftype ExternalSorter [^BufferAllocator allocator
                         vec-types
                         order-specs
                         ^long row-limit
                         ^Relation acc-rel
                         ^:unsynchronized-mutable ^Path sort-dir
                         ^:unsynchronized-mutable runs
                         ^:unsynchronized-mutable ^ICursor out]
  IExternalSorter
  (append [_ rel]
    (.append acc-rel rel)

    (when (and (<= row-limit (quot ^int *block-size* 2))
               (< ^int *block-size* (.getRowCount acc-rel)))
      (util/with-open [top-rel (.openDirectSlice (.select acc-rel (top-idxs acc-rel order-specs row-limit)) allocator)]
        (.clear acc-rel)
        (.append acc-rel top-rel)))

    (when (< ^int *block-size* (.getRowCount acc-rel))
      (when-not sort-dir
        (set! sort-dir (util/tmp-dir "external-sort")))
      (set! runs (conj runs (write-run allocator acc-rel order-specs row-limit sort-dir (count runs))))
      (.clear acc-rel)))

  (finish [_]
    (set! out
          (if (empty? runs)
            (InMemoryCursor. (when (pos? (.getRowCount acc-rel))
                               (.select acc-rel (top-idxs acc-rel order-specs row-limit))))

            (let [runs (cond-> runs
                         (pos? (.getRowCount acc-rel))
                         (conj (write-run allocator acc-rel order-specs row-limit sort-dir (count runs))))
                  sorted-file (merge-runs allocator runs order-specs row-limit vec-types (.toFile sort-dir))]
              (util/with-close-on-catch [loader (Relation/loader allocator (util/->file-channel (util/->path sorted-file)))]
                (SpilledCursor. loader (Relation. allocator (.getSchema loader)))))))
    out)

  (close [_]
    (util/try-close out)
    (util/try-close acc-rel)
    (when sort-dir (util/delete-dir sort-dir))))

(defn ->external-sorter
  "Sorts pages with the columns in `vec-types` by `order-specs`, keeping only the first `row-limit` rows.
  Spills a sorted run to a temp dir whenever it holds more than `*block-size*` rows."
  ^IExternalSorter [^BufferAllocator allocator, vec-types, order-specs, ^long row-limit]
  (util/with-close-on-catch [acc-rel (Relation. allocator
                                                ^List (vec (for [[col-name vec-type] vec-types]
                                                             (Vector/open allocator (str col-name) vec-type)))
                                                0)]
    (ExternalSorter. allocator vec-types order-specs row-limit acc-rel nil [] nil)))

(deftype OrderByCursor [^ICursor in-cursor
                        ^IExternalSorter sorter
                        ^:unsynchronized-mutable ^ICursor sorted]
  ICursor
  (getCursorType [_] "order-by")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [_ c]
    (when-not sorted
      (.forEachRemaining in-cursor (fn [^RelationReader rel] (.append sorter rel)))
      (set! sorted (.finish sorter)))
    (.tryAdvance sorted c))

  (close [_]
    (util/try-close sorter)
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^ICursor [allocator in-cursor vec-types order-specs row-limit]
  (OrderByCursor. in-cursor (->external-sorter allocator vec-types order-specs row-limit) nil))
