(ns xtdb.operator.patch
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.vector.writer :as vw])
  (:import [java.time Instant]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot
           xtdb.operator.PatchGapsCursor))

(s/def ::valid-from (partial instance? Instant))
(s/def ::valid-to (s/nilable (partial instance? Instant)))

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
         :->cursor (fn [{:keys [allocator]} inner]
                     (PatchGapsCursor. inner
                                       (vw/root->writer (VectorSchemaRoot/create (Schema. (for [[nm field] fields]
                                                                                            (types/field-with-name field (str nm))))
                                                                                 allocator))
                                       (time/instant->micros valid-from)
                                       (if valid-to
                                         (time/instant->micros valid-to)
                                         Long/MAX_VALUE)))}))))
