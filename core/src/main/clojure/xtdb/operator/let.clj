(ns xtdb.operator.let
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp])
  (:import [xtdb ICursor$Factory]))

(s/def ::binding (s/tuple simple-symbol? ::lp/ra-expression))

(defmethod lp/ra-expr :let [_]
  (s/cat :op #{:let}
         :binding ::binding
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :let [{[binding bound-rel] :binding, :keys [relation]} emit-opts]
  (let [{->bound-cursor :->cursor, :as emitted-bound-rel} (lp/emit-expr bound-rel emit-opts)
        {->body-cursor :->cursor, :as emitted-body-rel} (lp/emit-expr relation (assoc-in emit-opts [:let-bindings binding] emitted-bound-rel))]
    {:fields (:fields emitted-body-rel)
     :stats (:stats emitted-body-rel)
     :->cursor (fn [opts]
                 (->body-cursor (assoc-in opts [:let-bindings binding]
                                          (reify ICursor$Factory
                                            (open [_]
                                              (->bound-cursor opts))))))}))

(s/def ::relation simple-symbol?)
(s/def ::col-names (s/coll-of ::lp/column :kind vector?))

(defmethod lp/ra-expr :relation [_]
  (s/cat :op #{:relation}
         :relation ::relation
         :opts (s/keys :req-un [::col-names])))

(defmethod lp/emit-expr :relation [{:keys [relation]} emit-opts]
  (let [{:keys [fields stats]} (get-in emit-opts [:let-bindings relation])]
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
