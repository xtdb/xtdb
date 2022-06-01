(ns core2.operator.top
  (:require [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.vector.IIndirectRelation
           java.util.function.Consumer
           java.util.stream.IntStream))

(s/def ::skip nat-int?)
(s/def ::limit nat-int?)

(defmethod lp/ra-expr :top [_]
  (s/cat :op #{:λ :top}
         :top (s/keys :opt-un [::skip ::limit])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn offset+length [^long skip, ^long limit,
                     ^long idx, ^long row-count]
  (let [rel-offset (max (- skip idx) 0)
        consumed (max (- idx skip) 0)
        rel-length (min (- limit consumed)
                         (- row-count rel-offset))]
    (when (pos? rel-length)
      [rel-offset rel-length])))

(deftype TopCursor [^ICursor in-cursor
                    ^long skip
                    ^long limit
                    ^:unsynchronized-mutable ^long idx]
  ICursor
  (tryAdvance [this c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (< (- idx skip) limit)
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         row-count (.rowCount in-rel)
                                         old-idx (.idx this)]

                                     (set! (.-idx this) (+ old-idx row-count))

                                     (when-let [[^long rel-offset, ^long rel-length] (offset+length skip limit old-idx row-count)]
                                       (.accept c (iv/select in-rel (.toArray (IntStream/range rel-offset (+ rel-offset rel-length)))))
                                       (aset advanced? 0 true))))))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

(defmethod lp/emit-expr :top [{:keys [relation], {:keys [skip limit]} :top} args]
  (lp/unary-expr relation args
    (fn [col-types]
      {:col-types col-types
       :->cursor (fn [_opts in-cursor]
                   (TopCursor. in-cursor (or skip 0) (or limit Long/MAX_VALUE) 0))})))
