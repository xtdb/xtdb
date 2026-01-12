(ns xtdb.operator.list
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.expression.list :as expr-list]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           (xtdb.arrow ListExpression RelationReader Vector VectorType)))

(defmethod lp/ra-expr :list [_]
  (s/cat :op #{:list}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :list (s/map-of ::lp/column any?, :count 1)))


(defn- restrict-cols [vec-types {:keys [explicit-col-names]}]
  (cond-> vec-types
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat #xt/type :null))))
                           (select-keys explicit-col-names))))

(def ^:dynamic *batch-size* 1024)

(deftype ListCursor [^BufferAllocator allocator
                     ^ListExpression list-expr
                     ^String col-name
                     ^VectorType vec-type
                     ^long batch-size
                     ^:unsynchronized-mutable ^long current-pos]
  ICursor
  (getCursorType [_] "list")
  (getChildCursors [_] [])

  (tryAdvance [_ consumer]
    (boolean
     (when (and list-expr (< current-pos (.getSize list-expr)))
       (let [start current-pos
             end (min (.getSize list-expr) (+ current-pos batch-size))]
         (set! current-pos end)
         (util/with-open [out-vec (Vector/open allocator col-name vec-type)]
           (.writeTo list-expr out-vec start (- end start))
           (.accept consumer (vr/rel-reader [out-vec]))
           true)))))

  (close [_] nil))

(defmethod lp/emit-expr :list
  [{:keys [list] :as list-expr}
   {:keys [param-types schema] :as opts}]
  (let [[out-col v] (first list)
        input-types {:param-types param-types}
        expr (expr/form->expr v input-types)
        {:keys [vec-type ->list-expr]} (expr-list/compile-list-expr expr input-types)
        vec-types (restrict-cols {out-col vec-type} list-expr)]
    {:op :list
     :children []
     :vec-types vec-types
     :->cursor (fn [{:keys [allocator ^RelationReader args explain-analyze? tracer query-span]}]
                 (cond-> (ListCursor. allocator (->list-expr schema args)
                                      (str out-col) (get vec-types out-col)
                                      *batch-size* 0)
                   (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))
