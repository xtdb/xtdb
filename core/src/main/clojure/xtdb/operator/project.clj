(ns xtdb.operator.project
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (java.util LinkedHashMap)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.arrow VectorType)
           (xtdb.operator ProjectCursor ProjectionSpec$Factory ProjectionSpec$Identity ProjectionSpec$LocalRowNumber ProjectionSpec$RowNumber ProjectionSpec$Star)))

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

(defn ->identity-projection-spec ^ProjectionSpec$Factory [from-name col-type]
  (ProjectionSpec$Identity. (str from-name) col-type))

(defn projection-specs->vec-types
  "`projection-specs`: ordered `[col-name ProjectionSpec$Factory]` pairs."
  [projection-specs]
  (into {} (map (fn [[col-name ^ProjectionSpec$Factory spec]]
                  [(symbol col-name) (.getType spec)]))
        projection-specs))

(defn- open-projection-specs ^LinkedHashMap [{:keys [allocator args schema]} projection-specs]
  (let [factories (LinkedHashMap.)]
    (doseq [[col-name spec] projection-specs]
      (.put factories (str col-name) spec))

    (let [opened (util/safe-mapv (fn [^ProjectionSpec$Factory spec]
                                   (.open spec allocator schema args))
                                 (.values factories))
          out (LinkedHashMap.)]
      (doseq [[col-name spec] (map vector (.keySet factories) opened)]
        (.put out col-name spec))
      out)))

(defn ->project-cursor
  "Takes ownership of `in-cursor`, closing it if opening the specs throws."
  [opts in-cursor projection-specs]
  (util/with-close-on-catch [in-cursor in-cursor]
    (ProjectCursor. in-cursor (open-projection-specs opts projection-specs))))

(defmethod lp/emit-expr :project [{:keys [relation], {:keys [projections append-columns?]} :opts} {:keys [param-types] :as args}]
  (let [emitted-child-relation (lp/emit-expr relation args)]
    (-> (lp/unary-expr emitted-child-relation
          (fn [{inner-vec-types :vec-types :as inner-rel}]
            (let [projection-specs (concat (when append-columns?
                                             (for [[col-name col-type] inner-vec-types]
                                               [col-name (->identity-projection-spec col-name col-type)]))
                                           (for [[p-type arg] projections]
                                             (case p-type
                                               :column [arg (->identity-projection-spec arg (get inner-vec-types arg))]

                                               :row-number-column (let [[col-name _form] (first arg)]
                                                                    [col-name ProjectionSpec$RowNumber/INSTANCE])

                                               :local-row-number-column (let [[col-name _form] (first arg)]
                                                                          [col-name ProjectionSpec$LocalRowNumber/INSTANCE])

                                               :star (let [[col-name _star] (first arg)]
                                                       [col-name (ProjectionSpec$Star. (types/->type (into [:struct (update-keys inner-vec-types str)])))])

                                               :rename (let [[to-name from-name] (first arg)
                                                             from-type (get inner-vec-types from-name)]
                                                         (assert from-type (format "Field %s not found in relation, available %s" from-name (pr-str (keys inner-vec-types))))
                                                         [to-name (->identity-projection-spec from-name from-type)])

                                               :extend (let [[col-name form] (first arg)
                                                             input-types {:var-types inner-vec-types
                                                                          :param-types param-types}
                                                             expr (expr/form->expr form input-types)]
                                                         [col-name (expr/->expression-projection-spec expr input-types)]))))
                  col-mapping (->> projections
                                   (into {} (keep (fn [[p-type arg]]
                                                    (when (= p-type :rename)
                                                      (let [[to-name from-name] (first arg)]
                                                        [to-name from-name]))))))]
              (let [out-vec-types (projection-specs->vec-types projection-specs)]
                {:op :project
                 :children [inner-rel]
                 :explain {:project (pr-str (into [] (map second) projections))
                           :append? (boolean append-columns?)}
                 :vec-types out-vec-types
                 :stats (:stats emitted-child-relation)
                 :col-mapping col-mapping
                 :->cursor (fn [opts in-cursor]
                             (ProjectCursor. in-cursor (open-projection-specs opts projection-specs)))}))))
        (lp/with-col-mapping))))
(defmethod lp/emit-expr :map [{:keys [opts] :as op} args]
  (lp/emit-expr (assoc op :op :project :opts (assoc opts :append-columns? true)) args))

