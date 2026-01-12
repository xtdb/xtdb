(ns xtdb.operator.project
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types])
  (:import (org.apache.arrow.vector.types.pojo Field)
           (xtdb.arrow VectorType)
           (xtdb.operator ProjectCursor ProjectionSpec ProjectionSpec$Identity ProjectionSpec$LocalRowNumber ProjectionSpec$Rename ProjectionSpec$RowNumber ProjectionSpec$Star)
           (xtdb ICursor)))

(s/def ::append-columns? boolean?)

(s/def ::projections
  (s/coll-of (s/or :column ::lp/column
                   :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                   :local-row-number-column (s/map-of ::lp/column #{'(local-row-number)}, :conform-keys true, :count 1)
                   :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
                   ;; don't do this for args, because they aren't real cols
                   ;; the EE handles these through `:extend`
                   :rename (s/map-of ::lp/column (s/and ::lp/column
                                                        #(not (str/starts-with? (name %) "?"))
                                                        (complement '#{xtdb/postgres-server-version xtdb/xtdb-server-version xtdb/end-of-time}))
                                     :conform-keys true, :count 1)
                   :extend ::lp/column-expression)))

(defmethod lp/ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :opts (s/keys :req-un [::projections]
                       :opt-un [::append-columns?])
         :relation ::lp/ra-expression))

;; :explicit ns to allow distinct spec with same key name
(s/def :xtdb.operator.project.map/projections
  (s/coll-of (s/or :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                   :local-row-number-column (s/map-of ::lp/column #{'(local-row-number)}, :conform-keys true, :count 1)
                   :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
                   :extend ::lp/column-expression)
             :min-count 1))

(defmethod lp/ra-expr :map [_]
  (s/cat :op #{:ⲭ :chi :map}
         :opts (s/keys :req-un [:xtdb.operator.project.map/projections])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn ->identity-projection-spec ^ProjectionSpec [col-name col-type]
  (ProjectionSpec$Identity. (str col-name) col-type))

(defn ->project-cursor [{:keys [allocator args schema]} in-cursor projection-specs]
  (ProjectCursor. allocator in-cursor projection-specs schema args))

(defmethod lp/emit-expr :project [{:keys [relation], {:keys [projections append-columns?]} :opts} {:keys [param-types] :as args}]
  (let [emitted-child-relation (lp/emit-expr relation args)]
    (lp/unary-expr emitted-child-relation
      (fn [{inner-vec-types :vec-types :as inner-rel}]
        (let [projection-specs (concat (when append-columns?
                                         (for [[col-name col-type] inner-vec-types]
                                           (->identity-projection-spec col-name col-type)))
                                       (for [[p-type arg] projections]
                                         (case p-type
                                           :column (->identity-projection-spec arg (get inner-vec-types arg))

                                           :row-number-column (let [[col-name _form] (first arg)]
                                                                (ProjectionSpec$RowNumber. (str col-name)))

                                           :local-row-number-column (let [[col-name _form] (first arg)]
                                                                      (ProjectionSpec$LocalRowNumber. (str col-name)))

                                           :star (let [[col-name _star] (first arg)]
                                                   (ProjectionSpec$Star. (str col-name)
                                                                         (types/->type (into [:struct (update-keys inner-vec-types str)]))))

                                           :rename (let [[to-name from-name] (first arg)
                                                         from-type (get inner-vec-types from-name)]
                                                     (assert from-type (format "Field %s not found in relation, available %s" from-name (pr-str (keys inner-vec-types))))
                                                     (ProjectionSpec$Rename. (str from-name) (str to-name) from-type))

                                           :extend (let [[col-name form] (first arg)
                                                         input-types {:var-types inner-vec-types
                                                                      :param-types param-types}
                                                         expr (expr/form->expr form input-types)]
                                                     (expr/->expression-projection-spec col-name expr input-types)))))]
          (let [out-vec-types (->> projection-specs
                                   (into {} (map (juxt #(symbol (.getToName ^ProjectionSpec %))
                                                       #(.getType ^ProjectionSpec %)))))]
            {:op :project
             :children [inner-rel]
             :explain {:project (pr-str (into [] (map second) projections))
                       :append? (boolean append-columns?)}
             :vec-types out-vec-types
             :stats (:stats emitted-child-relation)
             :->cursor (fn [{:keys [explain-analyze? tracer query-span] :as opts} in-cursor]
                         (cond-> (->project-cursor opts in-cursor projection-specs)
                           (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))))))

(defmethod lp/emit-expr :map [{:keys [opts] :as op} args]
  (lp/emit-expr (assoc op :op :project :opts (assoc opts :append-columns? true)) args))

