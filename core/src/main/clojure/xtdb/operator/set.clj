(ns xtdb.operator.set
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.join :as join]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.util.stream.IntStream
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.operator.join BuildSide ComparatorFactory ProbeSide)))

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
  (getCursorType [_] "union-all")
  (getChildCursors [_] [left-cursor right-cursor])

  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1 false)]
      (loop []
        (if (or (.tryAdvance left-cursor
                             (fn [in-rels]
                               (let [non-empty-rels (filterv #(pos? (.getRowCount ^RelationReader %)) in-rels)]
                                 (when-not (empty? non-empty-rels)
                                   (aset advanced? 0 true)
                                   (.accept c non-empty-rels)))))
                (.tryAdvance right-cursor
                             (fn [in-rels]
                               (let [non-empty-rels (filterv #(pos? (.getRowCount ^RelationReader %)) in-rels)]
                                 (when-not (empty? non-empty-rels)
                                   (aset advanced? 0 true)
                                   (.accept c non-empty-rels))))))
          (if (aget advanced? 0)
            true
            (recur))
          false))))
  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :union-all [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-fields :fields :as left-rel} {right-fields :fields :as right-rel}]
                    {:op :union-all
                     :children [left-rel right-rel]
                     :fields (union-fields left-fields right-fields)
                     :->cursor (fn [{:keys [explain-analyze?]} left-cursor right-cursor]
                                 (cond-> (UnionAllCursor. left-cursor right-cursor)
                                   explain-analyze? (ICursor/wrapExplainAnalyze)))})))

(deftype IntersectionCursor [^ICursor left-cursor, ^ICursor right-cursor
                             ^BuildSide build-side, key-col-names
                             cmp-factory
                             difference?
                             ^:unsynchronized-mutable build-phase-ran?]
  ICursor
  (getCursorType [_] (if difference? "difference" "intersection"))
  (getChildCursors [_] [left-cursor right-cursor])

  (tryAdvance [this c]
    (when-not build-phase-ran?
      (.forEachRemaining right-cursor
                         (fn [^RelationReader in-rel]
                           (.append build-side in-rel)))
      (.end build-side)
      (set! (.build-phase-ran? this) true))


    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance left-cursor
                                (fn [^RelationReader probe-rel]
                                  (let [row-count (.getRowCount probe-rel)]
                                    (when (pos? row-count)
                                      (let [cmp (ComparatorFactory/build cmp-factory build-side probe-rel key-col-names)
                                            probe-side (ProbeSide. build-side probe-rel key-col-names cmp)
                                            idxs (IntStream/builder)]
                                        (.forEachIndexOf probe-side
                                                         (fn [probe-idx build-idx]
                                                           (when (cond-> (not= -1 build-idx)
                                                                   difference? not)
                                                             (.add idxs probe-idx)))
                                                         true)
                                        (let [idxs (.toArray (.build idxs))]
                                          (when-not (empty? idxs)
                                            (aset advanced? 0 true)
                                            (.accept c (.select probe-rel idxs)))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close build-side)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :intersect [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-fields :fields :as left-rel} {right-fields :fields :as right-rel}]
                    (let [fields (union-fields left-fields right-fields)
                          key-col-names (set (keys fields))]
                      {:op :intersect
                       :children [left-rel right-rel]
                       :fields fields
                       :->cursor (fn [{:keys [allocator explain-analyze?]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:fields left-fields
                                                                        :key-col-names key-col-names})]

                                     (cond-> (IntersectionCursor. left-cursor right-cursor
                                                                  build-side (mapv name key-col-names)
                                                                  (join/->cmp-factory {:fields right-fields
                                                                                       :key-col-names key-col-names})
                                                                  false false)
                                       explain-analyze? (ICursor/wrapExplainAnalyze))))}))))

(defmethod lp/emit-expr :difference [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-fields :fields :as left-rel} {right-fields :fields :as right-rel}]
                    (let [fields (union-fields left-fields right-fields)
                          key-col-names (set (keys fields))]
                      {:op :difference
                       :children [left-rel right-rel]
                       :fields fields
                       :->cursor (fn [{:keys [allocator explain-analyze?]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:fields left-fields
                                                                        :key-col-names key-col-names})]

                                     (cond-> (IntersectionCursor. left-cursor right-cursor
                                                                  build-side (mapv name key-col-names)
                                                                  (join/->cmp-factory {:fields right-fields
                                                                                       :key-col-names key-col-names})
                                                                  true false)
                                       explain-analyze? (ICursor/wrapExplainAnalyze))))}))))
