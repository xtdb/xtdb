(ns xtdb.operator.patch
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.vector.writer :as vw])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot
           xtdb.operator.PatchGapsCursor
           [xtdb.vector RelationReader]))

(s/def ::instantable
  (s/or :literal ::time/datetime-value
        :param ::lp/param))

(s/def ::valid-from (s/nilable ::instantable))
(s/def ::valid-to (s/nilable ::instantable))

(defn- ->instant [[tag value] {:keys [^RelationReader args] :as opts}]
  (-> (case tag
        :literal value
        :param (-> (.vectorForOrNull args (str value))
                   (.getObject 0)))
      (time/->instant opts)))

(defmethod lp/ra-expr :patch-gaps [_]
  (s/cat :op #{:patch-gaps}
         :opts (s/keys :req-un [::valid-from ::valid-to])
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :patch-gaps [{:keys [relation], {:keys [valid-from valid-to]} :opts} opts]
  (lp/unary-expr (lp/emit-expr relation opts)
    (fn [inner-fields]
      (let [fields (-> inner-fields
                       (update 'doc types/->nullable-field))]
        {:fields fields
         :->cursor (fn [{:keys [allocator current-time] :as qopts} inner]
                     (PatchGapsCursor. inner
                                       (vw/root->writer (VectorSchemaRoot/create (Schema. (for [[nm field] fields]
                                                                                            (types/field-with-name field (str nm))))
                                                                                 allocator))
                                       (time/instant->micros (->instant (or valid-from [:literal current-time]) qopts))
                                       (if valid-to
                                         (time/instant->micros (->instant valid-to qopts))
                                         Long/MAX_VALUE)))}))))
