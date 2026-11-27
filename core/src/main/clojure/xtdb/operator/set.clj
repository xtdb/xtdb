(ns xtdb.operator.set
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.join :as join]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.util.stream.IntStream
           (xtdb.api ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.operator.join BuildSide ProbeSide)
           (xtdb.operator.set SetOperator$UnionAll)))

(defmethod lp/ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- union-vec-types [left-vec-types right-vec-types]
  (when-not (= (set (keys left-vec-types)) (set (keys right-vec-types)))
    (throw (err/incorrect :union-incompatible-cols "union incompatible cols"
                          {:left-col-names (set (keys left-vec-types))
                           :right-col-names (set (keys right-vec-types))})))

  ;; NOTE: this overestimates types for intersection - if one side's string and the other int,
  ;; they statically can't intersect - but maybe that's one step too far for now.
  (merge-with types/merge-types left-vec-types right-vec-types))

(defmethod lp/emit-expr :union-all [{:keys [_opts left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [op (SetOperator$UnionAll. (update-keys left-vec-types str) (update-keys right-vec-types str))]
                      {:op :union-all
                       :children [left-rel right-rel]
                       :vec-types (update-keys (.getVecTypes op) symbol)
                       :->cursor (fn [_ left-cursor right-cursor]
                                   (.openCursor op left-cursor right-cursor))}))))

(deftype IntersectionCursor [^ICursor left-cursor, ^ICursor right-cursor
                             ^BuildSide build-side, ^ProbeSide probe-side
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
                                      (let [idxs (IntStream/builder)]
                                        (.forEachIndexOf (.forBatch probe-side probe-rel)
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
    (util/try-close probe-side)
    (util/try-close build-side)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :intersect [{:keys [_opts left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [out-vec-types (union-vec-types left-vec-types right-vec-types)
                          key-col-names (set (keys out-vec-types))]
                      {:op :intersect
                       :children [left-rel right-rel]
                       :vec-types out-vec-types
                       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:vec-types left-vec-types
                                                                        :key-col-names key-col-names})]

                                     (IntersectionCursor. left-cursor right-cursor
                                                          build-side
                                                          (ProbeSide. allocator build-side (mapv name key-col-names)
                                                                      (join/->cmp-factory {:build-vec-types left-vec-types
                                                                                           :probe-vec-types right-vec-types
                                                                                           :nulls-equal? true}))
                                                          false false)))}))))

(defmethod lp/emit-expr :difference [{:keys [_opts left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [out-vec-types (union-vec-types left-vec-types right-vec-types)
                          key-col-names (set (keys out-vec-types))]
                      {:op :difference
                       :children [left-rel right-rel]
                       :vec-types out-vec-types
                       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:vec-types left-vec-types
                                                                        :key-col-names key-col-names})]

                                     (IntersectionCursor. left-cursor right-cursor
                                                          build-side
                                                          (ProbeSide. allocator build-side (mapv name key-col-names)
                                                                      (join/->cmp-factory {:build-vec-types left-vec-types
                                                                                           :probe-vec-types right-vec-types
                                                                                           :nulls-equal? true}))
                                                          true false)))}))))
