(ns xtdb.operator.distinct
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import java.util.stream.IntStream
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           (xtdb.operator.distinct DistinctRelationMap)))

(defmethod lp/ra-expr :distinct [_]
  (s/cat :op #{:δ :distinct}
         :relation ::lp/ra-expression))

(deftype DistinctCursor [^ICursor in-cursor
                         ^DistinctRelationMap rel-map]
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

(defn ->relation-map ^DistinctRelationMap
  [^BufferAllocator allocator,
   {:keys [key-col-names store-full-build-rel?
           build-fields
           nil-keys-equal?
           param-fields args]
    :as opts}]
  (let [param-types (update-vals param-fields types/field->col-type)
        build-key-col-names (get opts :build-key-col-names key-col-names)

        schema (Schema. (-> build-fields
                            (cond-> (not store-full-build-rel?) (select-keys build-key-col-names))
                            (->> (mapv (fn [[field-name field]]
                                         (-> field (types/field-with-name (str field-name))))))))]

    (util/with-close-on-catch [rel-writer (vw/->rel-writer allocator schema)]
      (let [build-key-cols (mapv #(vw/vec-wtr->rdr (.vectorFor rel-writer (str %))) build-key-col-names)]
        (DistinctRelationMap. allocator
                              (map str build-key-col-names)
                              (boolean store-full-build-rel?)
                              rel-writer
                              build-key-cols
                              (boolean nil-keys-equal?)
                              (update-keys param-types str)
                              args
                              64
                              4)))))

(defmethod lp/emit-expr :distinct [{:keys [relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
                 (fn [inner-fields]
                   {:fields inner-fields
                    :->cursor (fn [{:keys [allocator]} in-cursor]
                                (DistinctCursor. in-cursor (->relation-map allocator
                                                                           {:build-fields inner-fields
                                                                            :key-col-names (set (keys inner-fields))
                                                                            :nil-keys-equal? true})))})))
