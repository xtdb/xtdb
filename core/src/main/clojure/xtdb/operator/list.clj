(ns xtdb.operator.list
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.expression.list :as expr-list]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.vector.types.pojo Field)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           (xtdb.arrow ListExpression RelationReader)))

(defmethod lp/ra-expr :list [_]
  (s/cat :op #{:list}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :list (s/map-of ::lp/column any?, :count 1)))


(defn- restrict-cols [fields {:keys [explicit-col-names]}]
  (cond-> fields
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat types/null-field))))
                           (select-keys explicit-col-names))))

(def ^:dynamic *batch-size* 1024)

(deftype ListCursor [^BufferAllocator allocator
                     ^ListExpression list-expr
                     ^Field field
                     ^long batch-size
                     ^:unsynchronized-mutable ^long current-pos]
  ICursor
  (tryAdvance [_ consumer]
    (boolean
     (when (and list-expr (< current-pos (.getSize list-expr)))
       (let [start current-pos
             end (min (.getSize list-expr) (+ current-pos batch-size))]
         (set! current-pos end)
         (util/with-open [out-vec (.createVector field allocator)]
           (let [out-vec-writer (vw/->writer out-vec)]
             (.writeTo list-expr out-vec-writer start (- end start))
             (.accept consumer (vr/rel-reader [(vw/vec-wtr->rdr out-vec-writer)]))
             true))))))

  (close [_] nil))

(defmethod lp/emit-expr :list
  [{:keys [list] :as list-expr}
   {:keys [param-fields schema] :as opts}]
  (let [[out-col v] (first list)
        param-types (update-vals param-fields types/field->col-type)
        input-types (assoc opts :param-types param-types)
        expr (expr/form->expr v input-types) 
        {:keys [field ->list-expr]} (expr-list/compile-list-expr expr input-types)
        named-field (types/field-with-name field (str out-col))
        fields {(symbol (.getName named-field)) named-field}]
    {:fields (restrict-cols fields list-expr)
     :->cursor (fn [{:keys [allocator ^RelationReader args]}]
                 (ListCursor. allocator (->list-expr schema args) named-field
                              *batch-size* 0))}))
