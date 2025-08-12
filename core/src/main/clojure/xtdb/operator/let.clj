(ns xtdb.operator.let
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import [xtdb ICursor$Factory]
           [xtdb.operator LetMatCursorFactory]))

(s/def ::binding (s/tuple simple-symbol? ::lp/ra-expression))

(defmethod lp/ra-expr :let-mat [_]
  (s/cat :op #{:let-mat}
         :binding ::binding
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :let-mat [{[binding bound-rel] :binding, :keys [relation]} emit-opts]
  (let [{->bound-cursor :->cursor, :as emitted-bound-rel} (lp/emit-expr bound-rel emit-opts)
        {->body-cursor :->cursor, :as emitted-body-rel} (lp/emit-expr relation (assoc-in emit-opts [:let-bindings binding] emitted-bound-rel))]
    {:fields (:fields emitted-body-rel)
     :stats (:stats emitted-body-rel)
     :->cursor (fn [{:keys [allocator] :as opts}]
                 (util/with-close-on-catch [bound-cursor (->bound-cursor opts)
                                            factory (LetMatCursorFactory. allocator bound-cursor)
                                            body-cursor (->body-cursor (assoc-in opts [:let-bindings binding] factory))]
                   (.wrapBodyCursor factory body-cursor)))}))

(s/def ::relation simple-symbol?)
(s/def ::col-names (s/coll-of ::lp/column :kind vector?))

(defmethod lp/ra-expr :relation [_]
  (s/cat :op #{:relation}
         :relation ::relation
         :opts (s/keys :req-un [::col-names])))

(defmethod lp/emit-expr :relation [{:keys [relation]} emit-opts]
  (let [{:keys [fields stats]} (or (get-in emit-opts [:let-bindings relation])
                                   (let [available (set (keys (:let-bindings emit-opts)))]
                                     (throw (err/fault ::missing-relation
                                                       (format "Can't find relation '%s', available %s"
                                                               relation available)
                                                       {:relation relation
                                                        :available available}))))]

    {:fields fields, :stats stats
     :->cursor (fn [opts]
                 (let [^ICursor$Factory cursor-factory (or (get-in opts [:let-bindings relation])
                                                           (let [available (set (keys (:let-bindings opts)))]
                                                             (throw (err/fault ::missing-relation
                                                                               (format "Can't find relation '%s', available %s"
                                                                                       relation available)
                                                                               {:relation relation
                                                                                :available available}))))]

                   (.open cursor-factory)))}))
