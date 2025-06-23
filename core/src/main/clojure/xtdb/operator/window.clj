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
           (com.carrotsearch.hppc LongLongHashMap LongLongMap)
           (java.io Closeable)
           (java.util LinkedList List Spliterator)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           (xtdb.arrow IntVector RelationReader)
           (xtdb.operator.group_by IGroupMapper)
           (xtdb.vector RelationWriter)))

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
              [:table [{}]]]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpec
  (^xtdb.arrow.VectorReader aggregate [^xtdb.arrow.VectorReader groupMapping
                                       ^ints sortMapping
                                       ^xtdb.arrow.RelationReader in-rel]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (^org.apache.arrow.vector.types.pojo.Field getToColumnField [])
  (^xtdb.operator.window.IWindowFnSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ^xtdb.operator.window.IWindowFnSpecFactory ->window-fn-factory
  (fn [{:keys [f from-name from-type to-name zero-row?]}]
    (expr/normalise-fn-name f)))

(defmethod ->window-fn-factory :row_number [{:keys [to-name]}]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnField [_] (types/col-type->field :i64))

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))
            ^LongLongMap group-to-cnt (LongLongHashMap.)]
        (reify
          IWindowFnSpec
          (aggregate [_ group-mapping sortMapping _in-rel]
            (let [offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (.setValueCount out-vec (+ offset row-count))
              (dotimes [idx row-count]
                (.set out-vec (+ offset idx) (.putOrAdd group-to-cnt (.getInt group-mapping (aget sortMapping idx)) 1 1)))
              (vr/vec->reader out-vec)))

          Closeable
          (close [_] (.close out-vec)))))))

(deftype WindowFnCursor [^BufferAllocator allocator
                         ^ICursor in-cursor
                         ^IPersistentMap static-fields
                         ^IGroupMapper group-mapper
                         order-specs
                         ^List window-specs
                         ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       (let [window-groups (gensym "window-groups")]
         ;; TODO we likely want to do some retaining here instead of copying
         (util/with-open [rel-wtr (RelationWriter. allocator (for [^Field field static-fields]
                                                               (vw/->writer (.createVector field allocator))))
                          group-mapping (IntVector. allocator (str window-groups) false)]

           (.forEachRemaining in-cursor (fn [in-rel]
                                          (vw/append-rel rel-wtr in-rel)
                                          (.append group-mapping (.groupMapping group-mapper in-rel))))

           (let [rel-rdr (vw/rel-wtr->rdr rel-wtr)
                 sort-mapping (order-by/sorted-idxs (RelationReader/from (conj (seq rel-rdr) group-mapping) (.getValueCount group-mapping))
                                                    (into [[window-groups]] order-specs))]
             (util/with-open [window-cols (->> window-specs
                                               (mapv #(.aggregate ^IWindowFnSpec % group-mapping sort-mapping rel-rdr)))]
               (let [out-rel (vr/rel-reader (concat (.select rel-rdr sort-mapping) window-cols))]
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
      (fn [fields]
        (let [window-fn-factories (vec (for [p projections]
                                         ;; ignoring window-name for now
                                         (let [[to-column {:keys [_window-name window-agg]}] (first p)]
                                           (->window-fn-factory (into {:to-name to-column
                                                                       :zero-row? (empty? partition-cols)}
                                                                      (zmatch window-agg
                                                                        [:nullary agg-opts]
                                                                        (select-keys agg-opts [:f])

                                                                        [:unary _agg-opts]
                                                                        (throw (UnsupportedOperationException.))))))))
              out-fields (-> (into fields
                                   (->> window-fn-factories
                                        (into {} (map (juxt #(.getToColumnName ^IWindowFnSpecFactory %)
                                                            #(.getToColumnField ^IWindowFnSpecFactory %)))))))]
          {:fields out-fields

           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (util/with-close-on-catch [window-fn-specs (LinkedList.)]
                         (doseq [^IWindowFnSpecFactory factory window-fn-factories]
                           (.add window-fn-specs (.build factory allocator)))

                         (WindowFnCursor. allocator in-cursor (order-by/rename-fields fields)
                                          (group-by/->group-mapper allocator (select-keys fields partition-cols))
                                          order-specs
                                          (vec window-fn-specs)
                                          false)))})))))
