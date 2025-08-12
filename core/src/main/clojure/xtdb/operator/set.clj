(ns xtdb.operator.set
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.join :as join]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.util.stream.IntStream
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.expression.map RelationMap)
           (xtdb.operator.join JoinRelationMap)))

(defmethod lp/ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- union-fields [left-fields right-fields]
  (when-not (= (set (keys left-fields)) (set (keys right-fields)))
    (throw (err/illegal-arg :union-incompatible-cols
                            {::err/message "union incompatible cols"
                             :left-col-names (set (keys left-fields))
                             :right-col-names (set (keys right-fields))})))

  ;; NOTE: this overestimates types for intersection - if one side's string and the other int,
  ;; they statically can't intersect - but maybe that's one step too far for now.
  (merge-with types/merge-fields left-fields right-fields))

(deftype UnionAllCursor [^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1 false)]
      (loop []
        (if (or (.tryAdvance left-cursor
                             (fn [^RelationReader in-rel]
                               (when (pos? (.getRowCount in-rel))
                                 (aset advanced? 0 true)
                                 (.accept c in-rel))))
                (.tryAdvance right-cursor
                             (fn [^RelationReader in-rel]
                               (when (pos? (.getRowCount in-rel))
                                 (aset advanced? 0 true)
                                 (.accept c in-rel)))))
          (if (aget advanced? 0)
            true
            (recur))
          false))))
  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :union-all [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [left-fields right-fields]
                    {:fields (union-fields left-fields right-fields)
                     :->cursor (fn [_opts left-cursor right-cursor]
                                 (UnionAllCursor. left-cursor right-cursor))})))

(deftype IntersectionCursor [^ICursor left-cursor
                             ^ICursor right-cursor
                             ^JoinRelationMap rel-map
                             difference?]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining right-cursor
                       (fn [^RelationReader in-rel]
                         (.append rel-map in-rel)))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance left-cursor
                                (fn [^RelationReader in-rel]
                                  (let [row-count (.getRowCount in-rel)
                                        prober (.probeFromRelation rel-map in-rel)]

                                    (when (pos? row-count)
                                      (let [idxs (IntStream/builder)]
                                        (dotimes [idx row-count]
                                          (when (cond-> (not= -1 (.indexOf prober idx true))
                                                  difference? not)
                                            (.add idxs idx)))

                                        (let [idxs (.toArray (.build idxs))]
                                          (when-not (empty? idxs)
                                            (aset advanced? 0 true)
                                            (.accept c (.select in-rel idxs)))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close rel-map)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :intersect [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
    (fn [left-fields right-fields]
      (let [fields (union-fields left-fields right-fields)]
        {:fields fields
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (join/->join-relation-map allocator
                                                                    {:build-fields left-fields
                                                                     :key-col-names (set (keys fields))})
                                          false))}))))

(defmethod lp/emit-expr :difference [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
    (fn [left-fields right-fields]
      (let [fields (union-fields left-fields right-fields)]
        {:fields fields
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (join/->join-relation-map allocator {:build-fields left-fields
                                                                               :key-col-names (set (keys fields))})
                                          true))}))))

