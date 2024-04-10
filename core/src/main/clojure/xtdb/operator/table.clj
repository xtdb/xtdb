(ns xtdb.operator.table
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql.edn :as edn])
  (:import clojure.lang.MapEntry
           (java.util ArrayList HashMap HashSet Set)
           java.util.function.Function
           (org.apache.arrow.vector.types.pojo ArrowType$Union Field)
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

(defn- ->out-rel [{:keys [allocator] :as opts} fields rows ->v]
  (let [row-count (count rows)]
    (when (pos? row-count)
      (util/with-close-on-catch [out-cols (ArrayList. (count fields))]
        (doseq [[col-name ^Field field] fields
                :let [col-kw (keyword col-name)
                      out-vec (.createVector field allocator)]]
          (util/with-close-on-catch [out-writer (vw/->writer out-vec)]

            (dotimes [idx row-count]
              (let [row (nth rows idx)
                    v (-> (get row col-kw) (->v opts))]
                (.writeObject out-writer v)))

            (.syncValueCount out-writer)
            (.add out-cols (.withName (vr/vec->reader out-vec) (str col-name)))))

        (vr/rel-reader out-cols row-count)))))

(defn- emit-rows-table [rows table-expr {:keys [param-fields] :as opts}]
  (let [param-types (update-vals param-fields types/field->col-type)
        field-sets (HashMap.)
        row-count (count rows)
        out-rows (ArrayList. row-count)
        key-fn #xt/key-fn :kebab-case-keyword
        rows (map #(update-keys % keyword) rows)
        columns (->> (mapcat keys rows)
                     (into #{}))]
    (doseq [row rows]
      (let [out-row (HashMap.)
            absent-columns (set/difference columns (set (keys row)))]

        (doseq [absent-column absent-columns
                :let [^Set field-set (.computeIfAbsent field-sets (symbol absent-column) (reify Function (apply [_ _] (HashSet.))))]]
          (.add field-set types/null-field))

        (doseq [[k v] row
                :let [k-sym (symbol k)]]
          (let [expr (expr/<-Expr (edn/parse-expr v) (assoc opts :param-types param-types))
                ^Set field-set (.computeIfAbsent field-sets k-sym (reify Function (apply [_ _] (HashSet.))))]
            (case (:op expr)
              :literal (do
                         (.add field-set (let [arrow-type (vw/value->arrow-type v)]
                                           (types/->field-default-name arrow-type (nil? v) nil)))
                         (.put out-row k v))

              :param (let [{:keys [param]} expr]
                       (.add field-set (get param-fields param))
                       ;; TODO let's try not to copy this out and back in again
                       (.put out-row k (fn [{:keys [^RelationReader params]}]
                                         (let [col (.readerForName params (name param))]
                                           (.getObject col 0 key-fn)))))

              ;; HACK: this is quite heavyweight to calculate a single value -
              ;; the EE doesn't yet have an efficient means to do so...
              (let [input-types (assoc opts :param-types param-types)
                    expr (expr/<-Expr (edn/parse-expr v) input-types)
                    projection-spec (expr/->expression-projection-spec "_scalar" expr input-types)]
                (.add field-set (types/col-type->field (.getColumnType projection-spec)))
                (.put out-row k (fn [{:keys [allocator params]}]
                                  (util/with-open [out-vec (.project projection-spec allocator (vr/rel-reader [] 1) params)]
                                    (.getObject out-vec 0 key-fn))))))))
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
                                            (= #xt.arrow/type :null (.getType el-leg))
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
                                   (instance? ArrowType$Union (.getType (.getField vec-rdr))) (.legReader :list))
                        el-rdr (some-> list-rdr .listElementReader)
                        el-struct-rdr (cond-> el-rdr
                                        (instance? ArrowType$Union (.getType (.getField el-rdr))) (.legReader :struct))
                        res (vr/rel-reader (for [k (some-> el-struct-rdr .structKeys)
                                                 :when (contains? fields (symbol k)) ]
                                             (.structKeyReader el-struct-rdr k))
                                           (.valueCount el-rdr))]
                    res))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [[{:keys [fields ->out-rel]} param?] (zmatch table
                                                    [:rows rows] [(emit-rows-table rows table-expr opts) false]
                                                    [:param param] [(emit-arg-table param table-expr opts) true])]

    {:fields fields
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts) param?))}))
