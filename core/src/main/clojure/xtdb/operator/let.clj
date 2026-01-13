(ns xtdb.operator.let
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import [xtdb ICursor ICursor$Factory]
           [xtdb.operator LetCursorFactory]))

(s/def ::binding-sym simple-symbol?)

(defmethod lp/ra-expr :let [_]
  (s/cat :op #{:let}
         :opts (s/keys :req-un [::binding-sym])
         :bound-relation ::lp/ra-expression
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :let [{{:keys [binding-sym]} :opts, :keys [bound-relation relation]} emit-opts]
  (let [{->bound-cursor :->cursor, :as emitted-bound-rel} (lp/emit-expr bound-relation emit-opts)
        {->body-cursor :->cursor, body-vec-types :vec-types, :as emitted-body-rel} (lp/emit-expr relation (assoc-in emit-opts [:let-bindings binding-sym] emitted-bound-rel))]
    {:op :let
     :children [emitted-bound-rel emitted-body-rel]
     :vec-types body-vec-types
     :stats (:stats emitted-body-rel)
     :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span] :as opts}]
                 (cond-> (util/with-close-on-catch [bound-cursor (->bound-cursor opts)
                                                    factory (LetCursorFactory. allocator bound-cursor)
                                                    body-cursor (->body-cursor (assoc-in opts [:let-bindings binding-sym] factory))]
                           (.wrapBodyCursor factory body-cursor))
                   (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))

(s/def ::cte-id simple-symbol?)
(s/def ::col-names (s/coll-of ::lp/column :kind vector?))

(defmethod lp/ra-expr :relation [_]
  (s/cat :op #{:relation}
         :opts (s/keys :req-un [::cte-id ::col-names])))

(defmethod lp/emit-expr :relation [{{:keys [cte-id]} :opts} emit-opts]
  (let [{:keys [vec-types stats]} (or (get-in emit-opts [:let-bindings cte-id])
                                       (let [available (set (keys (:let-bindings emit-opts)))]
                                         (throw (err/fault ::missing-relation
                                                           (format "Can't find relation '%s', available %s"
                                                                   cte-id available)
                                                           {:cte-id cte-id
                                                            :available available}))))]

    {:op :relation
     :children []
     :vec-types vec-types
     :stats stats
     :->cursor (fn [{:keys [explain-analyze? tracer query-span] :as opts}]
                 (let [^ICursor$Factory cursor-factory (or (get-in opts [:let-bindings cte-id])
                                                           (let [available (set (keys (:let-bindings opts)))]
                                                             (throw (err/fault ::missing-relation
                                                                               (format "Can't find relation '%s', available %s"
                                                                                       cte-id available)
                                                                               {:cte-id cte-id
                                                                                :available available}))))]

                   (cond-> (.open cursor-factory)
                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))}))
