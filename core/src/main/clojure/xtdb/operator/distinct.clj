(ns xtdb.operator.distinct
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util Map)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.operator.distinct DistinctRelationMap DistinctRelationMap$ComparatorFactory)))

(defmethod lp/ra-expr :distinct [_]
  (s/cat :op #{:δ :distinct}
         :opts map?
         :relation ::lp/ra-expression))

(deftype DistinctCursor [^BufferAllocator al
                         ^ICursor in-cursor
                         ^DistinctRelationMap rel-map]
  ICursor
  (getCursorType [_] "distinct")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (.tryAdvance in-cursor
                               (fn [^RelationReader in-rel]
                                 (let [row-count (.getRowCount in-rel)]
                                   (when (pos? row-count)
                                     (let [builder (.buildFromRelation rel-map in-rel)
                                           idxs (IntStream/builder)]
                                       (dotimes [idx row-count]
                                         (when (neg? (.addIfNotPresent builder idx))
                                           (.add idxs idx)))

                                       (let [idxs (.toArray (.build idxs))]
                                         (when-not (empty? idxs)
                                           (aset advanced? 0 true)
                                           (.accept c (.select in-rel idxs)))))))))))
      (aget advanced? 0)))

  (close [_]
    (util/try-close rel-map)
    (util/try-close in-cursor)))

(defn ->relation-map ^DistinctRelationMap
  [^BufferAllocator allocator,
   {:keys [key-col-names store-full-build-rel?
           build-vec-types
           nil-keys-equal?
           param-types args]
    :as opts}]
  (let [param-types (update-keys param-types str)
        build-key-col-names (get opts :build-key-col-names key-col-names)

        vec-types (-> build-vec-types
                      (cond-> (not store-full-build-rel?) (select-keys build-key-col-names))
                      (update-keys str))]

    (DistinctRelationMap. allocator ^Map vec-types
                          (map str build-key-col-names)
                          (boolean store-full-build-rel?)
                          (reify DistinctRelationMap$ComparatorFactory
                            (buildEqui [_ build-col in-col]
                              (emap/->equi-comparator build-col in-col
                                                      args
                                                      {:nil-keys-equal? nil-keys-equal?
                                                       :param-types param-types})))
                          64 4)))

(defmethod lp/emit-expr :distinct [{:keys [_opts relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
                 (fn [{inner-vec-types :vec-types :as inner-rel}]
                   {:op :distinct
                    :children [inner-rel]
                    :vec-types inner-vec-types
                    :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                                (cond-> (DistinctCursor. allocator in-cursor
                                                         (->relation-map allocator
                                                                         {:build-vec-types inner-vec-types
                                                                          :key-col-names (set (keys inner-vec-types))
                                                                          :nil-keys-equal? true}))
                                  (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))
