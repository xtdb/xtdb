(ns xtdb.operator.distinct
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import java.util.stream.IntStream
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.expression.map RelationMap)))

(defmethod lp/ra-expr :distinct [_]
  (s/cat :op #{:δ :distinct}
         :relation ::lp/ra-expression))

(deftype DistinctCursor [^ICursor in-cursor
                         ^RelationMap rel-map]
  ICursor
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

(defmethod lp/emit-expr :distinct [{:keys [relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
                 (fn [inner-fields]
                   {:fields inner-fields
                    :->cursor (fn [{:keys [allocator]} in-cursor]
                                (DistinctCursor. in-cursor (emap/->relation-map allocator
                                                                                {:build-fields inner-fields
                                                                                 :key-col-names (set (keys inner-fields))
                                                                                 :nil-keys-equal? true})))})))
