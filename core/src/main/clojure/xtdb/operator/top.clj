(ns xtdb.operator.top
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types])
  (:import java.util.stream.IntStream
           (xtdb ICursor)
           xtdb.arrow.RelationReader))

(s/def ::skip (s/nilable (s/or :literal nat-int?, :param ::lp/param)))
(s/def ::limit (s/nilable (s/or :literal nat-int?, :param ::lp/param)))

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
  (getCursorType [_] "top")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (< (- idx skip) limit)
                  (.tryAdvance in-cursor
                               (fn [^RelationReader in-rel]
                                 (let [row-count (.getRowCount in-rel)
                                       old-idx (.idx this)]

                                   (set! (.-idx this) (+ old-idx row-count))

                                   (when-let [[^long rel-offset, ^long rel-length] (offset+length skip limit old-idx row-count)]
                                     (.accept c (.select in-rel (.toArray (IntStream/range rel-offset (+ rel-offset rel-length)))))
                                     (aset advanced? 0 true)))))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

(defn- read-param [^RelationReader args param]
  (let [v (some-> args (.vectorForOrNull (str param)) (.getObject 0))]
    (if (nat-int? v)
      v
      (throw (err/incorrect :xtdb/expected-number (format "Expected: number, got: %s" v)
                            {:v v, :param param})))))

(defmethod lp/emit-expr :top [{:keys [relation], {[skip-tag skip-arg] :skip, [limit-tag limit-arg] :limit} :top} args]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [{vec-types :vec-types :as inner-rel}]
      {:op :top
       :children [inner-rel]
       :explain (->> {:skip (some-> skip-arg pr-str),
                      :limit (some-> limit-arg pr-str)}
                     (into {} (filter val)))
       :vec-types vec-types
       :fields (into {} (map (fn [[k v]] [k (types/->field v k)])) vec-types)
       :->cursor (fn [{:keys [args explain-analyze? tracer query-span]} in-cursor]
                   (cond-> (TopCursor. in-cursor
                                       (case skip-tag
                                         :literal skip-arg
                                         :param (read-param args skip-arg)
                                         nil 0)
                                       (case limit-tag
                                         :literal limit-arg
                                         :param (read-param args limit-arg)
                                         nil Long/MAX_VALUE)
                                       0)
                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))
