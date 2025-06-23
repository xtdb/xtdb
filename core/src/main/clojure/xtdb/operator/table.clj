(ns xtdb.operator.table
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.list :as expr-list]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           (java.util HashMap HashSet List Set)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema)
           (xtdb ICursor)
           (xtdb.arrow ListExpression Relation RelationReader RelationWriter VectorWriter)
           (xtdb.vector IRelationWriter)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :table (s/or :rows (s/coll-of (s/or :map (s/map-of simple-ident? any?)
                                             :param ::lp/param))
                      :column (s/map-of ::lp/column any?, :count 1)
                      :param ::lp/param)))

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

  (close [_]
    (util/close out-rel)))

(defn- restrict-cols [fields {:keys [explicit-col-names]}]
  (cond-> fields
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat types/null-field))))
                           (select-keys explicit-col-names))))

(defn- emit-rows-table [rows table-expr {:keys [param-fields schema] :as opts}]
  (let [param-types (update-vals param-fields types/field->col-type)
        field-sets (HashMap.)
        out-rows (->> rows
                      (mapv (fn [[row-tag row-arg]]
                              (case row-tag
                                :param (let [^Field struct-field (-> (for [^Field child-field (-> (or (get param-fields row-arg)
                                                                                                      (throw (UnsupportedOperationException. "missing param")))
                                                                                                  (types/flatten-union-field))
                                                                           :when (= #xt.arrow/type :struct (.getType child-field))]
                                                                       child-field)
                                                                     (->> (apply types/merge-fields)))
                                             ks (->> (.getChildren struct-field)
                                                     (into #{} (map #(symbol (.getName ^Field %)))))]

                                         (doseq [^Field struct-key (.getChildren struct-field)
                                                 :let [^Set field-set (.computeIfAbsent field-sets (symbol (.getName struct-key))
                                                                                        (fn [_] (HashSet.)))]]
                                           (.add field-set struct-key))

                                         {:ks ks
                                          :write-row! (fn write-param-row! [{:keys [^RelationReader args]}, ^IRelationWriter out-rel]
                                                        (let [param-rdr (.vectorForOrNull args (str row-arg))]
                                                          (.writeRow out-rel (.getObject param-rdr 0))))})

                                :map (let [out-row (->> row-arg
                                                        (into {}
                                                              (map (fn [[k v]]
                                                                     (let [k (symbol k)
                                                                           expr (expr/form->expr v (assoc opts :param-types param-types))
                                                                           ^Set field-set (.computeIfAbsent field-sets k (fn [_] (HashSet.)))]
                                                                       (case (:op expr)
                                                                         :literal (do
                                                                                    (.add field-set (types/col-type->field (vw/value->col-type v)))
                                                                                    (MapEntry/create k (fn write-literal! [_ ^VectorWriter out-col]
                                                                                                         (.writeObject out-col v))))

                                                                         :param (let [{:keys [param]} expr]
                                                                                  (.add field-set (get param-fields param))
                                                                                  (MapEntry/create k (fn write-param! [{:keys [^RelationReader args]} ^VectorWriter out-col]
                                                                                                       (.writeObject out-col
                                                                                                                     (-> (.vectorForOrNull args (str param))
                                                                                                                         (.getObject 0))))))

                                                                         ;; HACK: this is quite heavyweight to calculate a single value -
                                                                         ;; the EE doesn't yet have an efficient means to do so...
                                                                         (let [input-types (assoc opts :param-types param-types)
                                                                               expr (expr/form->expr v input-types)
                                                                               projection-spec (expr/->expression-projection-spec "_scalar" expr input-types)]
                                                                           (.add field-set (.getField projection-spec))
                                                                           (MapEntry/create k (fn write-expr! [{:keys [allocator args]} ^VectorWriter out-col]
                                                                                                (util/with-open [out-vec (.project projection-spec allocator (vr/rel-reader [] 1) schema args)]
                                                                                                  (.writeObject out-col (.getObject out-vec 0))))))))))))]

                                       {:ks (set (keys out-row))
                                        :write-row! (fn write-row! [opts ^RelationWriter out-rel]
                                                      (doseq [[k write-val!] out-row]
                                                        (write-val! opts (.vectorFor out-rel (str k))))
                                                      (.endRow out-rel))})))))

        key-freqs (->> (into [] (mapcat :ks) out-rows)
                       (frequencies))
        row-count (count out-rows)
        fields (-> field-sets
                   (->> (into {} (map (juxt key (fn [[k ^Set !v-types]]
                                                  (when-not (= row-count (get key-freqs (symbol k)))
                                                    (.add !v-types types/null-field))
                                                  (-> (apply types/merge-fields !v-types)
                                                      (types/field-with-name (str k))))))))
                   (restrict-cols table-expr))]

    {:fields fields
     :->out-rel (fn [{:keys [allocator] :as opts}]
                  (let [row-count (count rows)]
                    (when (pos? row-count)
                      (util/with-close-on-catch [root (VectorSchemaRoot/create (Schema. (or (vals fields) [])) allocator)
                                                 out-rel (vw/root->writer root)]
                        (doseq [{:keys [write-row!]} out-rows]
                          (write-row! opts out-rel))

                        (vw/rel-wtr->rdr out-rel)))))}))

(defn- emit-col-table [col-spec table-expr {:keys [param-fields schema] :as opts}]
  (let [[out-col v] (first col-spec)
        param-types (update-vals param-fields types/field->col-type)
        expr (expr/form->expr v (assoc opts :param-types param-types))
        input-types (assoc opts :param-types param-types)
        {:keys [field ->list-expr]} (expr-list/compile-list-expr expr input-types)
        named-field (types/field-with-name field (str out-col))]
    {:fields (-> {(symbol (.getName named-field)) named-field}
                 (restrict-cols table-expr))
     :->out-rel (fn [{:keys [allocator ^RelationReader args]}]
                  (util/with-close-on-catch [out-vec (.createVector named-field allocator)]
                    (let [^ListExpression list-expr (->list-expr schema args)
                          out-vec-writer (vw/->writer out-vec)] 
                      (when list-expr
                        (.writeTo list-expr out-vec-writer 0 (.getSize list-expr)))
                      (vr/rel-reader [(vw/vec-wtr->rdr out-vec-writer)]))))}))

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
     :->out-rel (fn [{:keys [^BufferAllocator allocator, ^RelationReader args]}]
                  (let [vec-rdr (.vectorForOrNull args (str (symbol param)))
                        list-rdr (cond-> vec-rdr
                                   (instance? ArrowType$Union (.getType (.getField vec-rdr))) (.vectorFor "list"))
                        el-rdr (some-> list-rdr (.getListElements))
                        el-struct-rdr (cond-> el-rdr
                                        (instance? ArrowType$Union (.getType (.getField el-rdr))) (.vectorFor "struct"))]

                    (vr/<-root (.openAsRoot (Relation. allocator
                                                       ^List (vec (for [k (some-> el-struct-rdr .getKeyNames)
                                                                        :when (contains? fields (symbol k))]
                                                                    (.vectorFor el-struct-rdr k)))
                                                       (.getValueCount el-rdr))
                                            allocator))))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [fields ->out-rel]} (zmatch table
                                     [:rows rows] (emit-rows-table rows table-expr opts)
                                     [:column col] (emit-col-table col table-expr opts)
                                     [:param param] (emit-arg-table param table-expr opts))]

    {:fields fields
     :->cursor (fn [opts]
                 (TableCursor. (->out-rel opts)))}))
