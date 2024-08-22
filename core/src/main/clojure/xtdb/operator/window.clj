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
           (com.carrotsearch.hppc LongLongMap LongLongHashMap)
           (java.io Closeable)
           (java.util LinkedList List Spliterator)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector)
           (xtdb ICursor)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.operator.group_by IGroupMapper)
           (xtdb.vector RelationWriter IVectorWriter)))

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
  (^xtdb.vector.IVectorReader aggregate [^org.apache.arrow.vector.IntVector groupMapping
                                         ^ints sortMapping
                                         ^xtdb.vector.RelationReader in-rel]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWindowFnSpecFactory
  (^clojure.lang.Symbol getToColumnName [])
  (getToColumnField [])
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
          (aggregate [_ group-mapping sortMapping]
            (let [offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (.setValueCount out-vec (+ offset row-count))
              (dotimes [idx row-count]
                (.set out-vec (+ offset idx) (.putOrAdd group-to-cnt (.get group-mapping (aget sortMapping idx)) 0 1)))
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

       (try
         (let [grouping-vec-name "grouping-vec"]
           ;; TODO we likely want to do some retaining here instead of copying
           (util/with-open [rel-wtr (RelationWriter. allocator (for [^Field field static-fields]
                                                                 (vw/->writer (.createVector field allocator))))
                            ;; TODO check why grouping can be null
                            ^IVectorWriter grouping-wtr (-> #xt.arrow/field ["grouping-vec" #xt.arrow/field-type [#xt.arrow/type :i32 true]]
                                                            (.createVector allocator)
                                                            vw/->writer)]

             (.forEachRemaining in-cursor (fn [in-rel]
                                            (vw/append-rel rel-wtr in-rel)
                                            (with-open [group-mapping (.groupMapping group-mapper in-rel)]
                                              (vw/append-vec grouping-wtr (vr/vec->reader group-mapping)))))

             (let [rel-rdr (vw/rel-wtr->rdr rel-wtr)
                   sort-mapping (order-by/sorted-idxs (vr/rel-reader (conj (seq rel-rdr) (vw/vec-wtr->rdr grouping-wtr)))
                                                      (into [[(symbol grouping-vec-name)]] order-specs))]
               (doseq [^IWindowFnSpec window-spec window-specs]
                 (.aggregate window-spec (.getVector grouping-wtr) sort-mapping))

               (util/with-open [window-cols (->> window-specs
                                                 (mapv #(.aggregate ^IWindowFnSpec % (.getVector grouping-wtr) sort-mapping rel-rdr)))]
                 (let [out-rel (vr/rel-reader (concat (seq (.select rel-rdr sort-mapping)) window-cols))]
                   (if (pos? (.rowCount out-rel))
                     (do
                       (.accept c out-rel)
                       true)
                     false))))))
         (finally
           (util/try-close group-mapper)
           (run! util/try-close window-specs))))))

  (characteristics [_] Spliterator/IMMUTABLE)

  (close [_]
    (run! util/try-close window-specs)
    (util/try-close in-cursor)
    (util/try-close group-mapper)))


(defmethod lp/emit-expr :window [{:keys [specs relation]} args]
  (let [window-spec (-> specs :windows first)
        _window-name (key window-spec)
        {:keys [partition-cols order-specs]} (val window-spec)
        projections (-> specs :projections)]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [fields]
        (let [window-fn-factories
              (for [p projections]
                ;; ignoring window-name for now
                (let [[to-column {:keys [_window-name window-agg]}] (first p)]
                  (->window-fn-factory (into {:to-name to-column
                                              :zero-row? (empty? partition-cols)}
                                             (zmatch window-agg
                                               [:nullary agg-opts]
                                               (select-keys agg-opts [:f])

                                               [:unary _agg-opts]
                                               (throw (UnsupportedOperationException.)))))))
              fields (-> (into fields
                               (->> window-fn-factories
                                    (into {} (map (juxt #(.getToColumnName ^IWindowFnSpecFactory  %)
                                                        #(.getToColumnField ^IWindowFnSpecFactory %)))))))]
          {:fields fields

           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (let [window-fn-specs (LinkedList.)]
                         (try
                           (doseq [^IWindowFnSpecFactory factory window-fn-factories]
                             (.add window-fn-specs (.build factory allocator)))

                           (WindowFnCursor. allocator in-cursor (order-by/rename-fields fields)
                                            (group-by/->group-mapper allocator (select-keys fields partition-cols))
                                            order-specs
                                            (vec window-fn-specs)
                                            false)

                           (catch Throwable e
                             (run! util/try-close window-fn-specs)
                             (throw e)))))})))))
