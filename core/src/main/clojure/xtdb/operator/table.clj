(ns xtdb.operator.table
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw])
  (:import (java.util ArrayList HashMap HashSet Set)
           java.util.function.Function
           (xtdb ICursor)
           (xtdb.vector IIndirectRelation)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :table-arg ::lp/param)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^:unsynchronized-mutable ^IIndirectRelation out-rel]
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

(defn- restrict-cols [col-types {:keys [explicit-col-names]}]
  (cond-> col-types
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat :null))))
                           (select-keys explicit-col-names))))

(defn- ->out-rel [{:keys [allocator] :as opts} col-types rows ->v]
  (let [row-count (count rows)]
    (when (pos? row-count)
      (util/with-close-on-catch [out-cols (ArrayList. (count col-types))]
        (doseq [[col-name col-type] col-types
                :let [col-kw (keyword col-name)
                      out-vec (-> (types/col-type->field col-name col-type)
                                  (.createVector allocator))
                      out-writer (vw/->writer out-vec)]]

          (.setInitialCapacity out-vec row-count)
          (.allocateNew out-vec)
          (.add out-cols (iv/->direct-vec out-vec))

          (dotimes [idx row-count]
            (let [row (nth rows idx)
                  v (-> (get row col-kw) (->v opts))]
              (vw/write-value! v (.writerForType out-writer (vw/value->col-type v)))))

          (.setValueCount out-vec row-count))

        (iv/->indirect-rel out-cols row-count)))))

(defn- emit-rows-table [rows table-expr {:keys [param-types] :as opts}]
  (let [col-type-sets (HashMap.)
        row-count (count rows)
        out-rows (ArrayList. row-count)]
    (doseq [row rows]
      (let [out-row (HashMap.)]
        (doseq [[k v] row
                :let [k-kw (keyword k)
                      k-sym (symbol k)]]
          (let [expr (expr/form->expr v opts)
                ^Set col-type-set (.computeIfAbsent col-type-sets k-sym (reify Function (apply [_ _] (HashSet.))))]
            (case (:op expr)
              :literal (do
                         (.add col-type-set (vw/value->col-type v))
                         (.put out-row k-kw v))

              :param (let [{:keys [param]} expr]
                       (.add col-type-set (get param-types param))
                       ;; TODO let's try not to copy this out and back in again
                       (.put out-row k-kw (fn [{:keys [^IIndirectRelation params]}]
                                            (let [col (.vectorForName params (name param))]
                                              (types/get-object (.getVector col) (.getIndex col 0))))))

              ;; HACK: this is quite heavyweight to calculate a single value -
              ;; the EE doesn't yet have an efficient means to do so...
              (let [projection-spec (expr/->expression-projection-spec "_scalar" v opts)]
                (.add col-type-set (.getColumnType projection-spec))
                (.put out-row k-kw (fn [{:keys [allocator params]}]
                                     (with-open [out-vec (.project projection-spec allocator (iv/->indirect-rel [] 1) params)]
                                       (types/get-object (.getVector out-vec) (.getIndex out-vec 0)))))))))
        (.add out-rows out-row)))

    (let [col-types (-> col-type-sets
                        (->> (into {} (map (juxt key (comp #(apply types/merge-col-types %) val)))))
                        (restrict-cols table-expr))]
      {:col-types col-types
       :->out-rel (fn [opts]
                    (->out-rel opts col-types out-rows
                               (fn [v opts]
                                 (if (fn? v) (v opts) v))))})))

(defn- emit-arg-table [param table-expr {:keys [table-arg-types]}]
  (let [col-types (-> (or (get table-arg-types param)
                          (throw (err/illegal-arg :unknown-table
                                                  {::err/message "Table refers to unknown param"
                                                   :param param, :table-args (set (keys table-arg-types))})))
                      (restrict-cols table-expr))]

    {:col-types col-types
     :->out-rel (fn [{:keys [table-args] :as opts}]
                  (->out-rel opts col-types (get table-args param) (fn [v _opts] v)))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [col-types ->out-rel]} (zmatch table
                                        [:rows rows] (emit-rows-table rows table-expr opts)
                                        [:table-arg param] (emit-arg-table param table-expr opts))]

    {:col-types col-types
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts)))}))
