(ns xtdb.operator.project
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types])
  (:import (org.apache.arrow.vector.types.pojo Field)
           (xtdb.operator ProjectCursor ProjectionSpec ProjectionSpec$Identity ProjectionSpec$LocalRowNumber ProjectionSpec$Rename ProjectionSpec$RowNumber ProjectionSpec$Star)
           (xtdb ICursor)))

(s/def ::append-columns? boolean?)

(defmethod lp/ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :opts (s/? (s/keys :req-un [::append-columns?]))
         :projections (s/coll-of (s/or :column ::lp/column
                                       :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :local-row-number-column (s/map-of ::lp/column #{'(local-row-number)}, :conform-keys true, :count 1)
                                       :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
                                       ;; don't do this for args, because they aren't real cols
                                       ;; the EE handles these through `:extend`
                                       :rename (s/map-of ::lp/column (s/and ::lp/column
                                                                            #(not (str/starts-with? (name %) "?"))
                                                                            (complement '#{xtdb/postgres-server-version xtdb/xtdb-server-version xtdb/end-of-time}))
                                                         :conform-keys true, :count 1)
                                       :extend ::lp/column-expression))
         :relation ::lp/ra-expression))

(defmethod lp/ra-expr :map [_]
  (s/cat :op #{:ⲭ :chi :map}
         :projections (s/coll-of (s/or :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :local-row-number-column (s/map-of ::lp/column #{'(local-row-number)}, :conform-keys true, :count 1)
                                       :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
                                       :extend ::lp/column-expression)
                                 :min-count 1)
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn ->identity-projection-spec ^ProjectionSpec [field]
  (ProjectionSpec$Identity. field))

(defn ->project-cursor [{:keys [allocator args schema]} in-cursor projection-specs]
  (ProjectCursor. allocator in-cursor projection-specs schema args))

(defmethod lp/emit-expr :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [param-fields] :as args}]
  (let [emitted-child-relation (lp/emit-expr relation args)]
    (lp/unary-expr emitted-child-relation
      (fn [{inner-fields :fields, :as inner-rel}]
        (let [projection-specs (concat (when append-columns?
                                         (for [[_col-name field] inner-fields]
                                           (->identity-projection-spec field)))
                                       (for [[p-type arg] projections]
                                         (case p-type
                                           :column (->identity-projection-spec (-> (get inner-fields arg)
                                                                                   (types/field-with-name (str arg))))

                                           :row-number-column (let [[col-name _form] (first arg)]
                                                                (ProjectionSpec$RowNumber. (str col-name)))

                                           :local-row-number-column (let [[col-name _form] (first arg)]
                                                                      (ProjectionSpec$LocalRowNumber. (str col-name)))

                                           :star (let [[col-name _star] (first arg)]
                                                   (ProjectionSpec$Star. (-> (types/->type (into [:struct] (vals inner-fields)))
                                                                             (types/->field (str col-name)))))

                                           :rename (let [[to-name from-name] (first arg)
                                                         field (some-> (get inner-fields from-name)
                                                                       (types/field-with-name (str to-name)))]
                                                     (assert field (format "Field %s not found in relation, available %s" from-name (pr-str (keys inner-fields))))
                                                     (ProjectionSpec$Rename. (str from-name) field))

                                           :extend (let [[col-name form] (first arg)
                                                         input-types {:vec-fields inner-fields, :param-fields param-fields}
                                                         expr (expr/form->expr form input-types)]
                                                     (expr/->expression-projection-spec col-name expr input-types)))))]
          {:op :project
           :children [inner-rel]
           :explain {:project (pr-str (into [] (map second) projections))
                     :append? (boolean append-columns?)}
           :fields (->> projection-specs
                        (into {} (map (comp (juxt #(symbol (.getName ^Field %)) identity)
                                            #(.getField ^ProjectionSpec %)))))
           :stats (:stats emitted-child-relation)
           :->cursor (fn [{:keys [explain-analyze? tracer query-span] :as opts} in-cursor]
                       (cond-> (->project-cursor opts in-cursor projection-specs)
                         explain-analyze? (ICursor/wrapExplainAnalyze)
                         (and tracer query-span) (ICursor/wrapTracing tracer query-span)))})))))

(defmethod lp/emit-expr :map [op args]
  (lp/emit-expr (assoc op :op :project :opts {:append-columns? true}) args))

