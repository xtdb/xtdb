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
  (:import (clojure.lang IPersistentMap MapEntry)
           (com.carrotsearch.hppc LongArrayList)
           (java.io Closeable)
           (java.util ArrayList List Map Spliterator HashMap)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector IntVector BigIntVector)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
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
            ^LongArrayList group-to-cnt (LongArrayList.)]
        (reify
          IWindowFnSpec
          (aggregate [_ group-mapping sortMapping _in-rel]
            (let [offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (.setValueCount out-vec (+ offset row-count))
              (dotimes [idx row-count]
                (let [sort-idx (aget sortMapping idx)
                      group (.get group-mapping sort-idx)
                      cnt (.get group-to-cnt group)]
                  (.set out-vec (+ offset sort-idx) cnt)
                  (.set group-to-cnt group (inc cnt))))
              (vr/vec->reader out-vec)))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->window-fn-factory :dense_rank [{:keys [to-name order-specs]}]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnField [_] (types/col-type->field :i64))

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))
            ^LongArrayList group-to-cnt (LongArrayList.)]
        (reify
          IWindowFnSpec
          (aggregate [_ group-mapping sortMapping in-rel]
            (let [cmp (order-by/order-specs->comp in-rel order-specs)
                  offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (.setValueCount out-vec (+ offset row-count))
              (when (pos-int? row-count)
                (.set out-vec (+ offset (aget sortMapping 0)) 0)
                (doseq [^long idx (range 1 row-count)]
                  (let [sort-idx (aget sortMapping idx)
                        p-sort-idx (aget sortMapping (dec idx))
                        group (.get group-mapping sort-idx)
                        previous-group (.get group-mapping p-sort-idx)
                        out-idx (+ offset sort-idx)]
                    (if (not= previous-group group)
                      (.set out-vec ^long out-idx 0)
                      (let [cnt (.get group-to-cnt group)]
                        (if (neg-int? (.compare cmp p-sort-idx sort-idx))
                          (do (.set out-vec out-idx (inc cnt))
                              (.set group-to-cnt group (inc cnt)))
                          (.set out-vec out-idx cnt)))))))
              (vr/vec->reader out-vec)))

          Closeable
          (close [_] (.close out-vec)))))))

(defmethod ->window-fn-factory :rank [{:keys [to-name order-specs]}]
  (reify IWindowFnSpecFactory
    (getToColumnName [_] to-name)
    (getToColumnField [_] (types/col-type->field :i64))

    (build [_ al]
      (let [^BigIntVector out-vec (-> (types/col-type->field to-name :i64)
                                      (.createVector al))
            ^LongArrayList group-to-cnt (LongArrayList.)]
        (reify
          IWindowFnSpec
          (aggregate [_ group-mapping sortMapping in-rel]
            (let [cmp (order-by/order-specs->comp in-rel order-specs)
                  offset (.getValueCount out-vec)
                  row-count (.getValueCount group-mapping)]
              (.setValueCount out-vec (+ offset row-count))
              (when (pos-int? row-count)
                (.set out-vec (+ offset (aget sortMapping 0)) 0)
                (when (< 0 row-count)
                  (loop [idx 1 gap 1]
                    (when (< idx row-count)
                      (let [sort-idx (aget sortMapping idx)
                            p-sort-idx (aget sortMapping (dec idx))
                            group (.get group-mapping sort-idx)
                            previous-group (.get group-mapping p-sort-idx)
                            out-idx (+ offset sort-idx)]
                        (if (not= previous-group group)
                          (do
                            (.set out-vec out-idx 0)
                            (recur (inc idx) 1))
                          (let [cnt (.get group-to-cnt group)]
                            (if (neg-int? (.compare cmp p-sort-idx sort-idx))
                              (do (.set out-vec out-idx (+ cnt gap))
                                  (.set group-to-cnt group (+ cnt gap))
                                  (recur (inc idx) 1))
                              (do (.set out-vec out-idx cnt)
                                  (recur (inc idx) (inc gap)))))))))))
              (vr/vec->reader out-vec)))

          Closeable
          (close [_] (.close out-vec)))))))


(deftype WindowFnCursor [^BufferAllocator allocator
                         ^ICursor in-cursor
                         ^IPersistentMap static-fields
                         ^Map window->group-mapper
                         ^Map window->order-specs
                         ^List window-specs
                         ^:unsynchronized-mutable ^boolean done?]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-not done?
       (set! (.done? this) true)

       (let [group-cnt (count window->group-mapper)
             window-groups (zipmap (keys window->group-mapper) (repeatedly group-cnt #(gensym "window-groups")))]
         ;; TODO we likely want to do some retaining here instead of copying
         (util/with-open [rel-wtr (RelationWriter. allocator (for [^Field field static-fields]
                                                               (vw/->writer (.createVector field allocator))))
                          group-wtrs (HashMap.)]

           (doseq [window-name (keys window->group-mapper)]
             (.put group-wtrs window-name
                   (-> (types/->field (str (get window-groups window-name)) #xt.arrow/type :i32 false)
                       (.createVector allocator)
                       vw/->writer)))

           (.forEachRemaining in-cursor (fn [in-rel]
                                          (vw/append-rel rel-wtr in-rel)
                                          (doseq [[window-name ^IGroupMapper group-mapper] window->group-mapper]
                                            (with-open [^IntVector group-mapping (.groupMapping group-mapper in-rel)]
                                              (vw/append-vec (get group-wtrs window-name) (vr/vec->reader group-mapping))))))

           (let [rel-rdr (vw/rel-wtr->rdr rel-wtr)
                 sort-mappings (into {}
                                     (map (fn [[window-name order-specs]]
                                            (MapEntry/create
                                             window-name
                                             (order-by/sorted-idxs (vr/rel-reader (conj (seq rel-rdr)
                                                                                        (vw/vec-wtr->rdr (get group-wtrs window-name))))
                                                                   (into [[(get window-groups window-name)]]
                                                                         order-specs)))))
                                     window->order-specs)]
             (util/with-open [window-cols (mapv (fn [[window-name ^IWindowFnSpec window-spec]]
                                                  (.aggregate window-spec
                                                              (.getVector ^IVectorWriter (get group-wtrs window-name))
                                                              (get sort-mappings window-name)
                                                              rel-rdr))
                                                window-specs)]
               (let [out-rel (vr/rel-reader (concat (seq rel-rdr) window-cols))]
                 (if (pos? (.rowCount out-rel))
                   (do
                     (.accept c out-rel)
                     true)
                   false)))))))))

  (characteristics [_] Spliterator/IMMUTABLE)

  (close [_]
    (util/close in-cursor)
    (util/close (map second window-specs))
    (util/close window->group-mapper)))

(defmethod lp/emit-expr :window [{:keys [specs relation]} args]
  (let [{:keys [projections windows]} specs]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [fields]
        (let [window-fn-factories (vec (for [p projections]
                                         ;; ignoring window-name for now
                                         (let [[to-column {:keys [window-name window-agg]}] (first p)
                                               {:keys [partition-cols order-specs]} (get windows window-name)]
                                           [window-name (->window-fn-factory (into {:to-name to-column
                                                                                    :order-specs order-specs
                                                                                    :zero-row? (empty? partition-cols)}
                                                                                   (zmatch window-agg
                                                                                     [:nullary agg-opts]
                                                                                     (select-keys agg-opts [:f])

                                                                                     [:unary _agg-opts]
                                                                                     (throw (UnsupportedOperationException.)))))])))
              out-fields (-> (into fields
                                   (->> window-fn-factories
                                        (into {} (map (comp (juxt #(.getToColumnName ^IWindowFnSpecFactory %)
                                                                  #(.getToColumnField ^IWindowFnSpecFactory %)) second))))))]

          {:fields out-fields

           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (util/with-close-on-catch [window-fn-specs (ArrayList.)
                                                  group-mappers (HashMap.)]
                         (doseq [[window-name ^IWindowFnSpecFactory factory] window-fn-factories]
                           (.add window-fn-specs [window-name (.build factory allocator)]))

                         (doseq [[window-name {:keys [partition-cols]}] windows]
                           (.put group-mappers window-name (group-by/->group-mapper allocator (select-keys fields partition-cols))))

                         (WindowFnCursor. allocator in-cursor (order-by/rename-fields fields)
                                          group-mappers
                                          (update-vals windows :order-specs)
                                          (vec window-fn-specs)
                                          false)))})))))
