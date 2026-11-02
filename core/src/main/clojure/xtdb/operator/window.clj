(ns xtdb.operator.window
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.expression.comparator :as expr.comp]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.group-by :as group-by]
            [xtdb.operator.order-by :as order-by]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IPersistentMap)
           (com.carrotsearch.hppc IntArrayList)
           (java.io Closeable)
           (java.util LinkedList List Map Spliterator)
           (java.util.function IntBinaryOperator)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.api ICursor)
           (xtdb.arrow IntVector LongVector RelationReader Relation Vector VectorReader)
           (xtdb.arrow.agg GroupMapper)))

(s/def ::window-name symbol?)

;; TODO
(s/def ::frame any?)

;; TODO assert at least one is present
(s/def ::partition-cols (s/coll-of ::lp/column :min-count 1))
(s/def ::window-spec (s/keys :opt-un [::partition-cols ::order-by/order-specs ::frame]))
(s/def ::windows (s/map-of ::window-name ::window-spec))

(s/def ::window-agg-expr
  (s/or :nullary (s/cat :f simple-symbol?)
        :unary (s/cat :f simple-symbol?
                      :from-column ::lp/column)
        :unary-with-offset (s/cat :f simple-symbol?
                                  :from-column ::lp/column
                                  :offset integer?)
        :binary (s/cat :f simple-symbol?
                       :left-column ::lp/column
                       :right-column ::lp/column)))

(s/def ::window-agg ::window-agg-expr)
(s/def ::window-projection (s/map-of ::lp/column (s/keys :req-un [::window-name ::window-agg])))
(s/def ::projections (s/coll-of ::window-projection :min-count 1))

(defmethod lp/ra-expr :window [_]
  (s/cat :op #{:window}
         :specs (s/keys :req-un [::windows ::projections])
         :relation ::lp/ra-expression))

(comment
  (s/valid? ::lp/logical-plan
            '[:window {:windows {window1 {:partition-cols [a]
                                          :order-specs [[b]]
                                          :frame {}}}
                       :projections [{col-name {:window-name window1
                                                :window-agg (row-number)}}]}
              [:table {:rows [{}]}]]))

(deftype SortedPartitions [^ints sortMapping
                           ^IntArrayList partitionStarts
                           ^IntArrayList peerStarts])

(defn- ->sorted-partitions
  "`partitionStarts` holds the sorted index at which each partition starts, with the row count as final sentinel:
  partition n runs from starts[n] (inclusive) to starts[n+1] (exclusive).

  `peerStarts` is the same for peer groups - runs of rows within a partition that are equal on every ORDER BY column.
  It is only computed when `peers?`, and is nil otherwise."
  ^xtdb.operator.window.SortedPartitions [^VectorReader group-mapping, ^ints sort-mapping, ^RelationReader in-rel, order-specs, peers?]
  (let [row-count (alength sort-mapping)
        ^"[Ljava.util.function.IntBinaryOperator;" order-cmps
        (into-array IntBinaryOperator
                    (map (fn [[col]]
                           (let [rdr (.vectorFor in-rel (str col))]
                             (expr.comp/->comparator rdr rdr :nulls-last)))
                         order-specs))
        n-cmps (alength order-cmps)
        partition-starts (IntArrayList.)
        peer-starts (when peers? (IntArrayList.))]
    (dotimes [sorted-idx row-count]
      (let [orig-idx (aget sort-mapping sorted-idx)
            group-id (.getInt group-mapping orig-idx)]
        (if (= (.size partition-starts) group-id)
          (do
            (.add partition-starts sorted-idx)
            (when peers? (.add peer-starts sorted-idx)))
          (when peers?
            (let [prev-idx (aget sort-mapping (dec sorted-idx))]
              (when-not (loop [i 0]
                          (or (== i n-cmps)
                              (and (zero? (.applyAsInt ^IntBinaryOperator (aget order-cmps i) prev-idx orig-idx))
                                   (recur (inc i)))))
                (.add peer-starts sorted-idx)))))))
    (.add partition-starts row-count)
    (when peers? (.add peer-starts row-count))
    (SortedPartitions. sort-mapping partition-starts peer-starts)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpec
  (^xtdb.arrow.VectorReader aggregate [^xtdb.operator.window.SortedPartitions sortedPartitions
                                       ^xtdb.arrow.RelationReader in-rel]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (^xtdb.arrow.VectorType getToColumnType [])
  (^boolean getNeedsPeers [])
  (^xtdb.operator.window.IWindowFnSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^xtdb.operator.window.IWindowFnSpecFactory ->window-fn-factory
  (fn [{:keys [f from-name from-type to-name zero-row?]}]
    (expr/normalise-fn-name f)))

(defmethod ->window-fn-factory :row_number [{:keys [to-name]}]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnType [_] #xt/type :i64)
    (getNeedsPeers [_] false)

    (build [_ al]
      (let [out-vec (LongVector. al (str to-name) false)]
        (reify
          IWindowFnSpec
          (aggregate [_ sorted-partitions _in-rel]
            (let [^IntArrayList partition-starts (.partitionStarts sorted-partitions)]
              (dotimes [part-idx (dec (.size partition-starts))]
                (let [partition-start (.get partition-starts part-idx)
                      partition-end (.get partition-starts (inc part-idx))]
                  (dotimes [within-part-idx (- partition-end partition-start)]
                    (.writeLong out-vec (inc within-part-idx)))))
              (.openSlice out-vec al)))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->window-fn-factory :lead [{:keys [to-name from-name from-type ^long offset]}]
  (let [out-type (types/->nullable-type from-type)]
    (reify IWindowFnSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] out-type)
      (getNeedsPeers [_] false)

      (build [_ al]
        (let [out-vec (Vector/open al (str to-name) out-type)]
          (reify IWindowFnSpec
            (aggregate [_ sorted-partitions in-rel]
              (let [from-vec (.vectorFor in-rel (str from-name))
                    row-copier (.rowCopier from-vec out-vec)
                    ^ints sort-mapping (.sortMapping sorted-partitions)
                    ^IntArrayList partition-starts (.partitionStarts sorted-partitions)
                    n-partitions (dec (.size partition-starts))]
                (dotimes [part-idx n-partitions]
                  (let [partition-start (.get partition-starts part-idx)
                        partition-end (.get partition-starts (inc part-idx))]
                    (dotimes [within-part-idx (- partition-end partition-start)]
                      (let [sorted-idx (+ partition-start within-part-idx)
                            lead-row-idx (+ sorted-idx offset)]
                        (if (< lead-row-idx partition-end)
                          (.copyRow row-copier (aget sort-mapping lead-row-idx))
                          (.writeNull out-vec)))))))

              (.openSlice out-vec al))

            Closeable
            (close [_] (.close out-vec))))))))

(defmethod ->window-fn-factory :lag [{:keys [to-name from-name from-type ^long offset]}]
  (let [out-type (types/->nullable-type from-type)]
    (reify IWindowFnSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] out-type)
      (getNeedsPeers [_] false)

      (build [_ al]
        (let [out-vec (Vector/open al (str to-name) out-type)]
          (reify IWindowFnSpec
            (aggregate [_ sorted-partitions in-rel]
              (let [from-vec (.vectorFor in-rel (str from-name))
                    row-copier (.rowCopier from-vec out-vec)
                    ^ints sort-mapping (.sortMapping sorted-partitions)
                    ^IntArrayList partition-starts (.partitionStarts sorted-partitions)
                    n-partitions (dec (.size partition-starts))]
                (dotimes [part-idx n-partitions]
                  (let [partition-start (.get partition-starts part-idx)
                        partition-end (.get partition-starts (inc part-idx))]
                    (dotimes [within-part-idx (- partition-end partition-start)]
                      (let [sorted-idx (+ partition-start within-part-idx)
                            lag-row-idx (- sorted-idx offset)]
                        (if (>= lag-row-idx partition-start)
                          (.copyRow row-copier (aget sort-mapping lag-row-idx))
                          (.writeNull out-vec)))))))

              (.openSlice out-vec al))

            Closeable
            (close [_] (.close out-vec))))))))

(defmacro ^:private peer-group-writer
  "A fn of `[out-vec sorted-partitions]` that runs `body` once per peer group, in sorted order.
  `ps`/`pe` bound the group's partition, `gs`/`ge` the group itself (sorted indices, end exclusive),
  and `ordinal` is the group's 1-based position within its partition."
  [[out-vec ps pe gs ge ordinal] & body]
  `(fn [~(vary-meta out-vec assoc :tag `Vector) ^xtdb.operator.window.SortedPartitions sp#]
     (let [^IntArrayList partition-starts# (.partitionStarts sp#)
           ^IntArrayList peer-starts# (.peerStarts sp#)
           n-groups# (dec (.size peer-starts#))]
       (loop [g# 0, part-idx# 0, ~ordinal 1]
         (when (< g# n-groups#)
           (let [~gs (.get peer-starts# g#)
                 ~ge (.get peer-starts# (inc g#))
                 part-idx# (if (>= ~gs (.get partition-starts# (inc part-idx#))) (inc part-idx#) part-idx#)
                 ~ps (.get partition-starts# part-idx#)
                 ~pe (.get partition-starts# (inc part-idx#))
                 ~ordinal (if (= ~gs ~ps) 1 ~ordinal)]
             ~@body
             (recur (inc g#) part-idx# (inc ~ordinal))))))))

(defn- ->peer-group-factory [to-name out-type write-groups!]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnType [_] out-type)
    (getNeedsPeers [_] true)

    (build [_ al]
      (let [out-vec (Vector/open al (str to-name) out-type)]
        (reify IWindowFnSpec
          (aggregate [_ sorted-partitions _in-rel]
            (write-groups! out-vec sorted-partitions)
            (.openSlice out-vec al))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->window-fn-factory :rank [{:keys [to-name]}]
  (->peer-group-factory to-name #xt/type :i64
                        (peer-group-writer [out-vec ps _pe gs ge _ordinal]
                          (dotimes [_ (- ge gs)]
                            (.writeLong out-vec (inc (- gs ps)))))))

(defmethod ->window-fn-factory :dense_rank [{:keys [to-name]}]
  (->peer-group-factory to-name #xt/type :i64
                        (peer-group-writer [out-vec _ps _pe gs ge ordinal]
                          (dotimes [_ (- ge gs)]
                            (.writeLong out-vec ordinal)))))

(defmethod ->window-fn-factory :percent_rank [{:keys [to-name]}]
  (->peer-group-factory to-name #xt/type :f64
                        (peer-group-writer [out-vec ps pe gs ge _ordinal]
                          (let [pct (if (= 1 (- pe ps))
                                      0.0
                                      (/ (double (- gs ps)) (double (dec (- pe ps)))))]
                            (dotimes [_ (- ge gs)]
                              (.writeDouble out-vec pct))))))

(defmethod ->window-fn-factory :cume_dist [{:keys [to-name]}]
  (->peer-group-factory to-name #xt/type :f64
                        (peer-group-writer [out-vec ps pe gs ge _ordinal]
                          (let [dist (/ (double (- ge ps)) (double (- pe ps)))]
                            (dotimes [_ (- ge gs)]
                              (.writeDouble out-vec dist))))))

(deftype WindowFnCursor [^BufferAllocator allocator
                         ^ICursor in-cursor
                         static-vec-types
                         ^GroupMapper group-mapper
                         order-specs
                         ^List window-specs
                         peers?
                         ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (getCursorType [_] "window")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       (let [window-groups (gensym "window-groups")]
         ;; TODO we likely want to do some retaining here instead of copying
         (util/with-open [out-rel (Relation. allocator ^Map (update-keys static-vec-types str))
                          group-mapping (IntVector/open allocator (str window-groups) false)]

           (.forEachRemaining in-cursor (fn [^RelationReader in-rel]
                                          (vw/append-rel out-rel in-rel)
                                          (.append group-mapping (.groupMapping group-mapper in-rel))))

           (let [sort-mapping (order-by/sorted-idxs (RelationReader/from (conj (seq out-rel) group-mapping) (.getValueCount group-mapping))
                                                    (into [[window-groups]] order-specs))
                 sorted-partitions (->sorted-partitions group-mapping sort-mapping out-rel order-specs peers?)]
             (util/with-open [window-cols (->> window-specs
                                               (mapv #(.aggregate ^IWindowFnSpec % sorted-partitions out-rel)))]
               (let [out-rel (vr/rel-reader (concat (.select out-rel sort-mapping) window-cols))]
                 (if (pos? (.getRowCount out-rel))
                   (do
                     (.accept c out-rel)
                     true)
                   false)))))))))

  (characteristics [_] Spliterator/IMMUTABLE)

  (close [_]
    (util/close [window-specs group-mapper in-cursor])))

(defmethod lp/emit-expr :window [{:keys [specs relation]} args]
  (let [{:keys [projections windows]} specs
        [_window-name {:keys [partition-cols order-specs]}] (first windows)]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [{:keys [vec-types] :as inner-rel}]
        (let [window-fn-factories (vec (for [p projections]
                                         ;; ignoring window-name for now
                                         (let [[to-column {:keys [_window-name window-agg]}] (first p)]
                                           (->window-fn-factory (into {:to-name to-column
                                                                       :zero-row? (empty? partition-cols)}
                                                                      (zmatch window-agg
                                                                        [:nullary agg-opts]
                                                                        agg-opts

                                                                        [:unary agg-opts]
                                                                        (assoc agg-opts
                                                                               :from-name (:from-column agg-opts)
                                                                               :from-type (get vec-types (:from-column agg-opts)))

                                                                        [:unary-with-offset agg-opts]
                                                                        (assoc agg-opts
                                                                               :from-name (:from-column agg-opts)
                                                                               :from-type (get vec-types (:from-column agg-opts)))))))))
              out-vec-types (into vec-types
                                  (->> window-fn-factories
                                       (into {} (map (juxt #(.getToColumnName ^IWindowFnSpecFactory %)
                                                           #(.getToColumnType ^IWindowFnSpecFactory %))))))]
          {:op :window
           :children [inner-rel]
           :explain {:partition-by (vec partition-cols)
                     :order-by (pr-str order-specs)
                     :window-functions (->> projections
                                            (mapv (fn [p]
                                                    (let [[to-column {:keys [window-agg]}] (first p)]
                                                      [to-column (pr-str window-agg)]))))}
           :vec-types out-vec-types

           :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                       (cond-> (util/with-close-on-catch [window-fn-specs (LinkedList.)]
                                 (doseq [^IWindowFnSpecFactory factory window-fn-factories]
                                   (.add window-fn-specs (.build factory allocator)))

                                 (WindowFnCursor. allocator in-cursor vec-types
                                                  (group-by/->group-mapper allocator (select-keys vec-types partition-cols))
                                                  order-specs
                                                  (vec window-fn-specs)
                                                  (boolean (some #(.getNeedsPeers ^IWindowFnSpecFactory %) window-fn-factories))
                                                  false))
                         (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))))
