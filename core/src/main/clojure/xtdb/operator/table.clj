(ns xtdb.operator.table
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util ArrayList HashMap HashSet Set)
           java.util.function.Function
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           (xtdb.vector RelationReader)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :table-arg ::lp/param)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^:unsynchronized-mutable ^RelationReader out-rel]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-let [out-rel out-rel]
       (try
         (set! (.out-rel this) nil)
         (.accept c out-rel)
         true
         (finally
           (.close out-rel))))))

  (close [_] (some-> out-rel .close)))

(defn- restrict-cols [fields {:keys [explicit-col-names]}]
  (cond-> fields
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat types/null-field))))
                           (select-keys explicit-col-names))))

(defn- ->out-rel [{:keys [allocator] :as opts} fields rows ->v]
  (let [row-count (count rows)]
    (when (pos? row-count)
      (util/with-close-on-catch [out-cols (ArrayList. (count fields))]
        (doseq [[col-name ^Field field] fields
                :let [col-kw (keyword col-name)
                      out-vec (.createVector (types/field-with-name field (str col-name)) allocator)
                      out-writer (vw/->writer out-vec)]]

          (.setInitialCapacity out-vec row-count)
          (.allocateNew out-vec)
          (.add out-cols (vr/vec->reader out-vec))

          (dotimes [idx row-count]
            (let [row (nth rows idx)
                  v (-> (get row col-kw) (->v opts))]
              (vw/write-value! v (.legWriter out-writer (vw/value->arrow-type v)))))

          (.syncValueCount out-writer))

        (vr/rel-reader out-cols row-count)))))

(defn- emit-rows-table [rows table-expr {:keys [param-fields] :as opts}]
  (let [param-types (update-vals param-fields types/field->col-type)
        field-sets (HashMap.)
        row-count (count rows)
        out-rows (ArrayList. row-count)]
    (doseq [row rows]
      (let [out-row (HashMap.)]
        (doseq [[k v] row
                :let [k-kw (keyword k)
                      k-sym (symbol k)]]
          (let [expr (expr/form->expr v (assoc opts :param-types param-types))
                ^Set field-set (.computeIfAbsent field-sets k-sym (reify Function (apply [_ _] (HashSet.))))]
            (case (:op expr)
              :literal (do
                         #_(prn {:v v
                                 :col-type (vw/value->col-type v)
                                 :field (types/->field-default-name (vw/value->arrow-type v) false nil)})
                         (.add field-set (types/->field-default-name (vw/value->arrow-type v) false nil))
                         (.put out-row k-kw v))

              :param (let [{:keys [param]} expr]
                       (.add field-set (get param-fields param))
                       ;; TODO let's try not to copy this out and back in again
                       (.put out-row k-kw (fn [{:keys [^RelationReader params]}]
                                            (let [col (.readerForName params (name param))]
                                              (.getObject col 0)))))

              ;; HACK: this is quite heavyweight to calculate a single value -
              ;; the EE doesn't yet have an efficient means to do so...
              (let [projection-spec (expr/->expression-projection-spec "_scalar" v (assoc opts :param-types param-types))]
                (.add field-set (types/col-type->field (.getColumnType projection-spec)))
                (.put out-row k-kw (fn [{:keys [allocator params]}]
                                     (with-open [out-vec (.project projection-spec allocator (vr/rel-reader [] 1) params)]
                                       (.getObject out-vec 0))))))))
        (.add out-rows out-row)))

    (let [fields (-> field-sets
                     (->> (into {} (map (juxt key (comp #(apply types/merge-fields %) val)))))
                     (restrict-cols table-expr))]
      {:fields fields
       :->out-rel (fn [opts]
                    (->out-rel opts fields out-rows
                               (fn [v opts]
                                 (if (fn? v) (v opts) v))))})))

(defn- emit-arg-table [param table-expr {:keys [table-arg-fields]}]
  (let [fields (-> (or (get table-arg-fields param)
                       (throw (err/illegal-arg :unknown-table
                                               {::err/message "Table refers to unknown param"
                                                :param param, :table-args (set (keys table-arg-fields))})))
                   (restrict-cols table-expr))]

    {:fields fields
     :->out-rel (fn [{:keys [table-args] :as opts}]
                  (->out-rel opts fields (get table-args param) (fn [v _opts] v)))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [fields ->out-rel]} (zmatch table
                                           [:rows rows] (emit-rows-table rows table-expr opts)
                                           [:table-arg param] (emit-arg-table param table-expr opts))]

    {:fields (doto fields)
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts)))}))
