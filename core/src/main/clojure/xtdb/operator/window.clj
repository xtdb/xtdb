(ns xtdb.operator.window
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.group-by :as group-by]
            [xtdb.operator.order-by :as order-by]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IPersistentMap)
           (com.carrotsearch.hppc IntArrayList LongLongHashMap LongLongMap)
           (java.io Closeable)
           (java.util LinkedList List Map Spliterator)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
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

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpec
  (^xtdb.arrow.VectorReader aggregate [^xtdb.arrow.VectorReader groupMapping
                                       ^ints sort-mapping
                                       ^xtdb.arrow.RelationReader in-rel]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (^xtdb.arrow.VectorType getToColumnType [])
  (^xtdb.operator.window.IWindowFnSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^xtdb.operator.window.IWindowFnSpecFactory ->window-fn-factory
  (fn [{:keys [f from-name from-type to-name zero-row?]}]
    (expr/normalise-fn-name f)))

(defmethod ->window-fn-factory :row_number [{:keys [to-name]}]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnType [_] #xt/type :i64)

    (build [_ al]
      (let [out-vec (LongVector. al (str to-name) false)
            ^LongLongMap group-to-cnt (LongLongHashMap.)]
        (reify
          IWindowFnSpec
          (aggregate [_ group-mapping sort-mapping _in-rel]
            (let [offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (dotimes [idx row-count]
                (.setLong out-vec (+ offset idx) (.putOrAdd group-to-cnt (.getInt group-mapping (aget sort-mapping idx)) 1 1)))
              (.openSlice out-vec al)))

          Closeable
          (close [_] (.close out-vec)))))))

(defn- build-partition-starts
  "Returns an IntArrayList of partition start indices, with row-count as final sentinel.
  Partition n runs from starts[n] (inclusive) to starts[n+1] (exclusive)."
  ^IntArrayList [^VectorReader group-mapping, ^ints sort-mapping]
  (let [row-count (alength sort-mapping)
        starts (IntArrayList.)]
    (dotimes [sorted-idx row-count]
      (let [orig-idx (aget sort-mapping sorted-idx)
            group-id (.getInt group-mapping orig-idx)]
        (when (= (.size starts) group-id)
          (.add starts sorted-idx))))
    (.add starts row-count)
    starts))

(defmethod ->window-fn-factory :lead [{:keys [to-name from-name from-type ^long offset]}]
  (let [out-type (types/->nullable-type from-type)]
    (reify IWindowFnSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] out-type)

      (build [_ al]
        (let [out-vec (Vector/open al (str to-name) out-type)]
          (reify IWindowFnSpec
            (aggregate [_ group-mapping sort-mapping in-rel]
              (let [from-vec (.vectorFor in-rel (str from-name))
                    row-copier (.rowCopier from-vec out-vec)
                    partition-starts (build-partition-starts group-mapping sort-mapping)
                    n-partitions (dec (.size partition-starts))]
                (dotimes [part-idx n-partitions]
                  (let [partition-start (.get partition-starts part-idx)
                        partition-end (.get partition-starts (inc part-idx))]
                    (dotimes [within-part-idx (- partition-end partition-start)]
                      (let [sorted-idx (+ partition-start within-part-idx)
                            lead-row-idx (+ sorted-idx offset)]
                        (if (< lead-row-idx partition-end)
                          (.copyRow row-copier (aget ^ints sort-mapping lead-row-idx))
                          (.writeNull out-vec)))))))

              (.openSlice out-vec al))

            Closeable
            (close [_] (.close out-vec))))))))

(defmethod ->window-fn-factory :lag [{:keys [to-name from-name from-type ^long offset]}]
  (let [out-type (types/->nullable-type from-type)]
    (reify IWindowFnSpecFactory
      (getToColumnName [_] to-name)
      (getToColumnType [_] out-type)

      (build [_ al]
        (let [out-vec (Vector/open al (str to-name) out-type)]
          (reify IWindowFnSpec
            (aggregate [_ group-mapping sort-mapping in-rel]
              (let [from-vec (.vectorFor in-rel (str from-name))
                    row-copier (.rowCopier from-vec out-vec)
                    partition-starts (build-partition-starts group-mapping sort-mapping)
                    n-partitions (dec (.size partition-starts))]
                (dotimes [part-idx n-partitions]
                  (let [partition-start (.get partition-starts part-idx)
                        partition-end (.get partition-starts (inc part-idx))]
                    (dotimes [within-part-idx (- partition-end partition-start)]
                      (let [sorted-idx (+ partition-start within-part-idx)
                            lag-row-idx (- sorted-idx offset)]
                        (if (>= lag-row-idx partition-start)
                          (.copyRow row-copier (aget ^ints sort-mapping lag-row-idx))
                          (.writeNull out-vec)))))))

              (.openSlice out-vec al))

            Closeable
            (close [_] (.close out-vec))))))))

(defn- invert-sort-mapping
  "Create inverse mapping: if sort-mapping[i] = j, then inverse[j] = i.
   This converts from sorted order back to original row order."
  ^ints [^ints sort-mapping]
  (let [n (alength sort-mapping)
        inverse (int-array n)]
    (dotimes [i n]
      (aset inverse (aget sort-mapping i) i))
    inverse))

(defn- compose-mappings
  "Given a value at position i in sorted order A, find where it should go in sorted order B.
   inverse-a[sort-b[i]] gives the position in A that corresponds to position i in B."
  ^ints [^ints inverse-a ^ints sort-b]
  (let [n (alength sort-b)
        result (int-array n)]
    (dotimes [i n]
      (aset result i (aget inverse-a (aget sort-b i))))
    result))

(deftype WindowFnCursor [^BufferAllocator allocator
                         ^ICursor in-cursor
                         static-vec-types
                         ;; Map of window-name -> {:group-mapper GroupMapper :order-specs ...}
                         ^IPersistentMap window-name->spec
                         ;; Vector of {:window-name ... :spec IWindowFnSpec}
                         ^List window-specs-with-names
                         ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (getCursorType [_] "window")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       ;; TODO we likely want to do some retaining here instead of copying
       (util/with-open [out-rel (Relation. allocator ^Map (update-keys static-vec-types str))]
         ;; Build group mappings for each window spec
         (let [group-mapping-vecs (into {} (map (fn [[window-name _]]
                                                  [window-name (IntVector/open allocator (str (gensym "window-groups")) false)]))
                                        window-name->spec)]
           (util/with-close-on-catch [_ (vals group-mapping-vecs)]
             (.forEachRemaining in-cursor
                                (fn [^RelationReader in-rel]
                                  (vw/append-rel out-rel in-rel)
                                  (doseq [[window-name {:keys [^GroupMapper group-mapper]}] window-name->spec]
                                    (let [^IntVector group-mapping-vec (get group-mapping-vecs window-name)]
                                      (.append group-mapping-vec (.groupMapping group-mapper in-rel))))))

             ;; Build sort mappings for each window spec (sort by group first, then by order-specs)
             (let [sort-mappings (into {} (map (fn [[window-name {:keys [order-specs]}]]
                                                 (let [^IntVector group-mapping-vec (get group-mapping-vecs window-name)
                                                       rel-with-groups (RelationReader/from (conj (seq out-rel) group-mapping-vec)
                                                                                            (.getValueCount group-mapping-vec))]
                                                   [window-name (order-by/sorted-idxs rel-with-groups
                                                                                      (into [[(symbol (.getName group-mapping-vec))]]
                                                                                            order-specs))])))
                                       window-name->spec)
                   ;; Pick a canonical sort mapping for final output (use first window's)
                   first-window-name (first (keys window-name->spec))
                   ^ints canonical-sort-mapping (get sort-mappings first-window-name)]

               ;; Compute window columns - each is computed in its own sorted order,
               ;; then reordered to match the canonical sort order
               (util/with-open [window-cols
                                (vec (for [{:keys [window-name ^IWindowFnSpec spec]} window-specs-with-names]
                                       (let [^IntVector group-mapping-vec (get group-mapping-vecs window-name)
                                             ^ints sort-mapping (get sort-mappings window-name)
                                             ;; Compute window values in this spec's sorted order
                                             window-col (.aggregate spec group-mapping-vec sort-mapping out-rel)]
                                         (if (identical? sort-mapping canonical-sort-mapping)
                                           ;; Same sort order - use directly
                                           window-col
                                           ;; Different sort order - reorder to canonical order
                                           ;; Window col values are in sort-mapping order
                                           ;; We need them in canonical-sort-mapping order
                                           ;; inverse[canonical[i]] gives where in window-col to get value for row i in output
                                           (let [reorder-mapping (compose-mappings (invert-sort-mapping sort-mapping)
                                                                                   canonical-sort-mapping)]
                                             (.select window-col reorder-mapping))))))]

                 (util/close (vals group-mapping-vecs))

                 (let [out-rel (vr/rel-reader (concat (.select out-rel canonical-sort-mapping) window-cols))]
                   (if (pos? (.getRowCount out-rel))
                     (do
                       (.accept c out-rel)
                       true)
                     false))))))))))

  (characteristics [_] Spliterator/IMMUTABLE)

  (close [_]
    (util/close [(map :spec window-specs-with-names)
                 (map :group-mapper (vals window-name->spec))
                 in-cursor])))

(defmethod lp/emit-expr :window [{:keys [specs relation]} args]
  (let [{:keys [projections windows]} specs]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [{:keys [vec-types] :as inner-rel}]
        (let [;; Build window-fn-factories with their window-name attached
              window-fn-factories-with-names
              (vec (for [p projections]
                     (let [[to-column {:keys [window-name window-agg]}] (first p)
                           {:keys [partition-cols]} (get windows window-name)]
                       {:window-name window-name
                        :factory (->window-fn-factory (into {:to-name to-column
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
                                                                     :from-type (get vec-types (:from-column agg-opts))))))})))

              out-vec-types (into vec-types
                                  (->> window-fn-factories-with-names
                                       (into {} (map (fn [{:keys [^IWindowFnSpecFactory factory]}]
                                                       [(.getToColumnName factory) (.getToColumnType factory)])))))]
          {:op :window
           :children [inner-rel]
           :explain {:windows (into {} (map (fn [[window-name {:keys [partition-cols order-specs]}]]
                                              [window-name {:partition-by (vec partition-cols)
                                                            :order-by (pr-str order-specs)}]))
                                    windows)
                     :window-functions (->> projections
                                            (mapv (fn [p]
                                                    (let [[to-column {:keys [window-name window-agg]}] (first p)]
                                                      [to-column {:window window-name :agg (pr-str window-agg)}]))))}
           :vec-types out-vec-types

           :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                       (cond-> (util/with-close-on-catch [window-fn-specs (LinkedList.)]
                                 ;; Build window specs with their window-name
                                 (doseq [{:keys [window-name ^IWindowFnSpecFactory factory]} window-fn-factories-with-names]
                                   (.add window-fn-specs {:window-name window-name
                                                          :spec (.build factory allocator)}))

                                 ;; Build window-name->spec map with group-mappers
                                 (let [window-name->spec
                                       (into {} (map (fn [[window-name {:keys [partition-cols order-specs]}]]
                                                       [window-name {:group-mapper (group-by/->group-mapper allocator
                                                                                                            (select-keys vec-types partition-cols))
                                                                     :order-specs order-specs}]))
                                             windows)]
                                   (WindowFnCursor. allocator in-cursor vec-types
                                                    window-name->spec
                                                    (vec window-fn-specs)
                                                    false)))
                         (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))))
