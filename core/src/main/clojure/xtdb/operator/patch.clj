(ns xtdb.operator.patch
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.vector.writer :as vw])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot
           [xtdb.arrow RelationReader]
           xtdb.operator.PatchGapsCursor))

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
                     (let [valid-from (time/instant->micros (->instant (or valid-from [:literal current-time]) qopts))
                           valid-to (or (some-> valid-to (->instant qopts) time/instant->micros) Long/MAX_VALUE)]
                       (if (> valid-from valid-to)
                         (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                               "Invalid valid times"
                                               {:valid-from (time/micros->instant  valid-from)
                                                :valid-to (time/micros->instant valid-to)}))
                         (PatchGapsCursor. inner
                                           (vw/root->writer (VectorSchemaRoot/create (Schema. (for [[nm field] fields]
                                                                                                (types/field-with-name field (str nm))))
                                                                                     allocator))
                                           valid-from
                                           valid-to))))}))))
