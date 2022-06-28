(ns core2.operator.table
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.rewrite :refer [zmatch]]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation)
           (java.util ArrayList HashSet HashMap Set)
           java.util.function.Function
           (org.apache.arrow.vector.complex DenseUnionVector)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind set?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :param ::lp/param)))

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
      (let [out-cols (ArrayList. (count col-types))]
        (try
          (doseq [[col-name col-type] col-types
                  :let [col-kw (keyword col-name)
                        out-vec (-> (types/col-type->field col-name col-type)
                                    (.createVector allocator))
                        duv? (instance? DenseUnionVector out-vec)
                        out-writer (vw/vec->writer out-vec)]]
            (.add out-cols (iv/->direct-vec out-vec))
            (util/set-value-count out-vec row-count)
            (dotimes [idx row-count]
              (let [row (nth rows idx)
                    v (-> (get row col-kw) (->v opts))]
                (.startValue out-writer)
                (if duv?
                  (doto (.writerForType (.asDenseUnion out-writer) (types/value->col-type v))
                    (.startValue)
                    (->> (types/write-value! v))
                    (.endValue))
                  (types/write-value! v out-writer))
                (.endValue out-writer))))
          (iv/->indirect-rel out-cols row-count)
          (catch Throwable e
            (run! util/try-close out-cols)
            (throw e)))))))

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
                         (.add col-type-set (types/value->col-type v))
                         (.put out-row k-kw v))

              :param (let [{:keys [param]} expr]
                       (.add col-type-set (get param-types param))
                       (.put out-row k-kw (fn [{:keys [params]}]
                                            (get params param))))

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

(defn- param-type->col-types [param-type]
  (letfn [(->struct-cols-inner [col-type]
            (zmatch col-type
              [:struct struct-cols] #{struct-cols}))

          (->struct-cols-outer [col-type]
            (zmatch col-type
              [:list [:union inner-types]] (->> inner-types (into #{} (mapcat ->struct-cols-inner)))
              [:list inner-type] (->struct-cols-inner inner-type)
              [:fixed-size-list _ [:union inner-types]] (->> inner-types (into #{} (mapcat ->struct-cols-inner)))
              [:fixed-size-list _ inner-type] (->struct-cols-inner inner-type)
              [:union inner-types] (->> inner-types (into #{} (mapcat ->struct-cols-outer)))))]

    (let [struct-cols (->struct-cols-outer param-type)]
      (->> (for [table-key (into #{} (mapcat keys) struct-cols)]
             [(symbol table-key) (->> (into #{} (map #(get % table-key :null)) struct-cols)
                                      (apply types/merge-col-types))])
           (into {})))))

(defn- emit-param-table [param table-expr {:keys [param-types]}]
  (let [col-types (-> (or (get param-types param)
                          (throw (err/illegal-arg :unknown-table
                                                  {::err/message "Table refers to unknown param"
                                                   :param param, :params (set (keys param-types))})))
                      (param-type->col-types)
                      (restrict-cols table-expr))]

    {:col-types col-types
     :->out-rel (fn [{:keys [params] :as opts}]
                  (->out-rel opts col-types (get params param) (fn [v _opts] v)))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [col-types ->out-rel]} (zmatch table
                                        [:rows rows] (emit-rows-table rows table-expr opts)
                                        [:param param] (emit-param-table param table-expr opts))]

    {:col-types col-types
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts)))}))
