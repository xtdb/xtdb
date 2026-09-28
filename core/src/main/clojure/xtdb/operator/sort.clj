(ns xtdb.operator.sort
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.order-by :as order-by]
            [xtdb.operator.top])
  (:import (xtdb.api ICursor)
           xtdb.arrow.RelationReader
           xtdb.operator.order_by.OrderByCursor
           xtdb.operator.top.TopCursor))

(s/def ::skip (s/nilable (s/or :literal nat-int?, :param ::lp/param)))
(s/def ::limit (s/nilable (s/or :literal nat-int?, :param ::lp/param)))

(defmethod lp/ra-expr :sort [_]
  (s/cat :op #{:sort}
         :opts (s/keys :opt-un [::order-by/order-specs ::skip ::limit])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype SortCursor [^ICursor in-cursor, ^ICursor out-cursor]
  ICursor
  (getCursorType [_] "sort")
  (getChildCursors [_] [in-cursor])
  (tryAdvance [_ c] (.tryAdvance out-cursor c))
  (close [_] (.close out-cursor)))

(defn- read-param [^RelationReader args param]
  (let [v (some-> args (.vectorForOrNull (str param)) (.getObject 0))]
    (if (nat-int? v)
      v
      (throw (err/incorrect :xtdb/expected-number (format "Expected: number, got: %s" v)
                            {:v v, :param param})))))

(defn- resolve-arg [[tag arg] args default]
  (case tag
    :literal arg
    :param (read-param args arg)
    nil default))

(defn- row-limit ^long [^long skip, ^long limit]
  (if (< (- Long/MAX_VALUE skip) limit)
    Long/MAX_VALUE
    (+ skip limit)))

(defmethod lp/emit-expr :sort [{{:keys [order-specs skip limit]} :opts, :keys [relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [{:keys [vec-types], :as inner-rel}]
      {:op :sort
       :children [inner-rel]
       :explain (->> {:order-specs (some-> (not-empty order-specs) pr-str)
                      :skip (some-> (second skip) pr-str)
                      :limit (some-> (second limit) pr-str)}
                     (into {} (filter val)))
       :vec-types vec-types
       :->cursor (fn [{:keys [allocator args explain-analyze? tracer query-span]} in-cursor]
                   (let [skip-n (resolve-arg skip args 0)
                         limit-n (resolve-arg limit args Long/MAX_VALUE)]
                     (cond-> (SortCursor. in-cursor
                                          (as-> in-cursor cursor
                                            (if (seq order-specs)
                                              (OrderByCursor. allocator cursor vec-types order-specs (row-limit skip-n limit-n)
                                                              false nil nil nil nil)
                                              cursor)
                                            (if (or skip limit)
                                              (TopCursor. cursor skip-n limit-n 0)
                                              cursor)))
                       (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))})))
