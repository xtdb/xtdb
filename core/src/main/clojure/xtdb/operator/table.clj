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
  (:import clojure.lang.MapEntry
           (java.util ArrayList HashMap HashSet Set)
           java.util.function.Function
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           (xtdb.vector RelationReader)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :param ::lp/param)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^:unsynchronized-mutable ^RelationReader out-rel, param?]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-let [out-rel out-rel]
       (try
         (set! (.out-rel this) nil)
         (.accept c out-rel)
         true
         (finally
           (when-not param? ; params get closed at toplevel
             (.close out-rel)))))))

  (close [_] (some-> out-rel .close)))

(defn- restrict-cols [fields {:keys [explicit-col-names]}]
  (cond-> fields
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat types/null-field))))
                           (select-keys explicit-col-names))))

(defn- create-field-without-children ^Field [col-name ^Field field]
  (let [arrow-type (.getType field)]
    (case arrow-type
      ;; lists need a child
      (#xt.arrow/type :list #xt.arrow/type :set) (types/->field (str col-name) arrow-type (.isNullable field)
                                                                (types/->field "$data$" #xt.arrow/type :union false))
      (types/->field (str col-name) arrow-type (.isNullable field)))))

(defn- ->out-rel [{:keys [allocator] :as opts} fields rows ->v]
  (let [row-count (count rows)]
    (when (pos? row-count)
      (util/with-close-on-catch [out-cols (ArrayList. (count fields))]
        (doseq [[col-name ^Field field] fields
                :let [col-kw (keyword col-name)
                      ;; create things dynamically here for now, to not run into issues with normalisation
                      out-vec (.createVector (create-field-without-children col-name field) allocator)
                      out-writer (vw/->writer out-vec)]]

          (dotimes [idx row-count]
            (let [row (nth rows idx)
                  v (-> (get row col-kw) (->v opts))]
              (vw/write-value! v (.legWriter out-writer (vw/value->arrow-type v)))))

          (.syncValueCount out-writer)
          (.add out-cols (vr/vec->reader out-vec)))

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
                         (.add field-set (let [arrow-type (vw/value->arrow-type v)]
                                           (types/->field-default-name arrow-type (nil? v)
                                                                       ;; can change once sets are treated explicitly in the EE
                                                                       (when (= arrow-type #xt.arrow/type :set)
                                                                         [(types/->field "$data$" #xt.arrow/type :union false)]))))
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

(defn- emit-arg-table [param table-expr {:keys [param-fields]}]
  (let [fields (-> (into {} (for [^Field field (-> (or (get param-fields param)
                                                       (throw (err/illegal-arg :unknown-table
                                                                               {::err/message "Table refers to unknown param"
                                                                                :param param, :params (set (keys param-fields))})))
                                                   (types/flatten-union-field))
                                  :when (or (= #xt.arrow/type :list (.getType field))
                                            (throw (err/illegal-arg :illegal-param-type
                                                                    {::err/message "Table param must be of type struct list"
                                                                     :param param})))
                                  :let [^Field el-field (first (.getChildren field))]
                                  ^Field el-leg (types/flatten-union-field el-field)
                                  :when (or (= #xt.arrow/type :struct (.getType el-leg))
                                            (throw (err/illegal-arg :illegal-param-type
                                                                    {::err/message "Table param must be of type struct list"
                                                                     :param param})))
                                  ^Field field (.getChildren el-leg)]
                              (MapEntry/create (symbol (.getName field)) field)))

                   (restrict-cols table-expr))]

    {:fields fields
     :->out-rel (fn [{:keys [^RelationReader params]}]
                  (let [vec-rdr (.readerForName params (str (symbol param)))
                        list-rdr (cond-> vec-rdr
                                   (.legs vec-rdr) (.legReader :list))
                        el-rdr (some-> list-rdr .listElementReader)
                        el-struct-rdr (cond-> el-rdr
                                        (.legs el-rdr) (.legReader :struct))
                        res (vr/rel-reader (for [k (some-> el-struct-rdr .structKeys)
                                                 :let [datalog-k (util/normal-form-str->datalog-form-str k)
                                                       in-datalog-form? (contains? fields (symbol datalog-k))]
                                                 :when (or (contains? fields (symbol k)) in-datalog-form?)]
                                             (cond-> (.structKeyReader el-struct-rdr k)
                                               in-datalog-form? (.withName datalog-k)))
                                           (.valueCount el-rdr))]
                    res))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [[{:keys [fields ->out-rel]} param?] (zmatch table
                                                    [:rows rows] [(emit-rows-table rows table-expr opts) false]
                                                    [:param param] [(emit-arg-table param table-expr opts) true])]

    {:fields fields
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts) param?))}))
