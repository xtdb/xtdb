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
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
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

(deftype UnionAllCursor [^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (getCursorType [_] "union-all")
  (getChildCursors [_] [left-cursor right-cursor])

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

(defmethod lp/emit-expr :union-all [{:keys [_opts left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [out-vec-types (union-vec-types left-vec-types right-vec-types)]
                      {:op :union-all
                       :children [left-rel right-rel]
                       :vec-types out-vec-types
                       :->cursor (fn [{:keys [explain-analyze? tracer query-span]} left-cursor right-cursor]
                                   (cond-> (UnionAllCursor. left-cursor right-cursor)
                                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))))

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
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [out-vec-types (union-vec-types left-vec-types right-vec-types)
                          key-col-names (set (keys out-vec-types))]
                      {:op :intersect
                       :children [left-rel right-rel]
                       :vec-types out-vec-types
                       :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:vec-types left-vec-types
                                                                        :key-col-names key-col-names})]

                                     (cond-> (IntersectionCursor. left-cursor right-cursor
                                                                  build-side (mapv name key-col-names)
                                                                  (join/->cmp-factory {:build-vec-types left-vec-types
                                                                                       :probe-vec-types right-vec-types})
                                                                  false false)
                                       (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))}))))

(defmethod lp/emit-expr :difference [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [out-vec-types (union-vec-types left-vec-types right-vec-types)
                          key-col-names (set (keys out-vec-types))]
                      {:op :difference
                       :children [left-rel right-rel]
                       :vec-types out-vec-types
                       :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} left-cursor right-cursor]
                                   (let [build-side (join/->build-side allocator
                                                                       {:vec-types left-vec-types
                                                                        :key-col-names key-col-names})]

                                     (cond-> (IntersectionCursor. left-cursor right-cursor
                                                                  build-side (mapv name key-col-names)
                                                                  (join/->cmp-factory {:build-vec-types left-vec-types
                                                                                       :probe-vec-types right-vec-types})
                                                                  true false)
                                       (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))}))))
