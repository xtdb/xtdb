(ns xtdb.operator.patch
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.time :as time]
            [xtdb.types :as types])
  (:import org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           (xtdb ICursor)
           [xtdb.arrow Relation RelationReader]
           xtdb.operator.PatchGapsCursor))

(s/def ::instantable
  (s/or :now #{:now '(current-timestamp)}
        :literal ::time/datetime-value
        :param ::lp/param))

(s/def ::valid-from (s/nilable ::instantable))
(s/def ::valid-to (s/nilable ::instantable))

(defn- ->instant [[tag value] {:keys [^RelationReader args] :as opts}]
  (-> (case tag
        :literal value
        :param (-> (.vectorForOrNull args (str value))
                   (.getObject 0))
        :now (expr/current-time))
      (time/->instant opts)))

(defmethod lp/ra-expr :patch-gaps [_]
  (s/cat :op #{:patch-gaps}
         :opts (s/keys :req-un [::valid-from ::valid-to])
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr :patch-gaps [{:keys [relation], {:keys [valid-from valid-to]} :opts} opts]
  (lp/unary-expr (lp/emit-expr relation opts)
    (fn [{inner-fields :fields :as inner-rel}]
      (let [fields (-> inner-fields
                       (update 'doc types/->nullable-field))]
        {:op :patch-gaps
         :children [inner-rel]
         :explain {:valid-from (pr-str valid-from)
                   :valid-to (pr-str valid-to)}
         :fields fields
         :->cursor (fn [{:keys [^BufferAllocator allocator current-time explain-analyze?] :as qopts} inner]
                     (let [valid-from (time/instant->micros (->instant (or valid-from [:literal current-time]) qopts))
                           valid-to (or (some-> valid-to (->instant qopts) time/instant->micros) Long/MAX_VALUE)]
                       (if (> valid-from valid-to)
                         (throw (err/incorrect :xtdb.indexer/invalid-valid-times
                                               "Invalid valid times"
                                               {:valid-from (time/micros->instant valid-from)
                                                :valid-to (time/micros->instant valid-to)}))
                         (cond-> (PatchGapsCursor. inner
                                                   (Relation. allocator
                                                              (Schema. (for [[nm field] fields]
                                                                         (types/field-with-name field (str nm)))))
                                                   valid-from
                                                   valid-to)
                           explain-analyze? (ICursor/wrapExplainAnalyze)))))}))))
