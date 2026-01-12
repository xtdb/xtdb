(ns xtdb.operator.table
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.list :as expr-list]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import clojure.lang.MapEntry
           (java.util HashMap HashSet List Set)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo ArrowType$Union)
           (xtdb ICursor)
           (xtdb.arrow ListExpression Relation RelationReader RelationWriter Vector VectorType VectorWriter)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind vector?))
         :table (s/or :rows (s/coll-of (s/or :map (s/map-of simple-ident? any?)
                                             :param ::lp/param))
                      :column (s/map-of ::lp/column any?, :count 1)
                      :param ::lp/param)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator al,
                      ^:unsynchronized-mutable ^Relation out-rel]
  ICursor
  (getCursorType [_] "table")
  (getChildCursors [_] [])

  (tryAdvance [this c]
    (boolean
     (when out-rel
       (try
         (set! (.out-rel this) nil)
         (.accept c out-rel)
         true
         (finally
           (.close out-rel))))))

  (close [_]
    (util/close out-rel)))

(defn- restrict-cols [vec-types {:keys [explicit-col-names]}]
  (cond-> vec-types
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat #xt/type :null))))
                           (select-keys explicit-col-names))))

(defn- emit-rows-table [rows table-expr {:keys [param-types schema] :as opts}]
  (let [type-sets (HashMap.)
        out-rows (->> rows
                      (mapv (fn [[row-tag row-arg]]
                              (case row-tag
                                :param (let [^VectorType struct-type (-> (for [^VectorType leg-type (or (get param-types row-arg)
                                                                                                            (throw (UnsupportedOperationException. "missing param")))
                                                                               :when (= #xt.arrow/type :struct (.getArrowType leg-type))]
                                                                           leg-type)
                                                                         (->> (apply types/merge-types)))
                                             children (.getChildren struct-type)
                                             ks (into #{} (map symbol) (keys children))]

                                         (doseq [[child-name ^VectorType child-type] children
                                                 :let [^Set type-set (.computeIfAbsent type-sets (symbol child-name)
                                                                                       (fn [_] (HashSet.)))]]
                                           (.add type-set child-type))

                                         {:ks ks
                                          :write-row! (fn write-param-row! [{:keys [^RelationReader args]}, ^RelationWriter out-rel]
                                                        (let [param-rdr (.vectorForOrNull args (str row-arg))]
                                                          (.writeRow out-rel (.getObject param-rdr 0))))})

                                :map (let [out-row (->> row-arg
                                                        (into {}
                                                              (map (fn [[k v]]
                                                                     (let [k (symbol k)
                                                                           expr (try
                                                                                  (expr/form->expr v (assoc opts :param-types param-types))
                                                                                  (catch Exception e
                                                                                    (log/warn "fail" {:expr v, :param-types param-types})
                                                                                    (throw e)))
                                                                           ^Set type-set (.computeIfAbsent type-sets k (fn [_] (HashSet.)))]
                                                                       (case (:op expr)
                                                                         :literal (do
                                                                                    (.add type-set (types/value->vec-type v))
                                                                                    (MapEntry/create k (fn write-literal! [_ ^VectorWriter out-col]
                                                                                                         (.writeObject out-col v))))

                                                                         :param (let [{:keys [param]} expr
                                                                                      ^VectorType param-type (get param-types param)]
                                                                                  (.add type-set param-type)
                                                                                  (MapEntry/create k (fn write-param! [{:keys [^RelationReader args]} ^VectorWriter out-col]
                                                                                                       (.writeObject out-col
                                                                                                                     (-> (.vectorForOrNull args (str param))
                                                                                                                         (.getObject 0))))))

                                                                         ;; HACK: this is quite heavyweight to calculate a single value -
                                                                         ;; the EE doesn't yet have an efficient means to do so...
                                                                         (let [input-types (assoc opts :param-types param-types)
                                                                               expr (expr/form->expr v input-types)
                                                                               projection-spec (expr/->expression-projection-spec "_scalar" expr input-types)]
                                                                           (.add type-set (.getType projection-spec))
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
        vec-types (-> type-sets
                      (->> (into {} (map (juxt key (fn [[k ^Set !v-types]]
                                                     (when-not (= row-count (get key-freqs (symbol k)))
                                                       (.add !v-types #xt/type :null))
                                                     (apply types/merge-types !v-types))))))
                      (restrict-cols table-expr))]

    {:vec-types vec-types
     :row-count row-count
     :->out-rel (fn [{:keys [^BufferAllocator allocator] :as opts}]
                  (let [row-count (count rows)]
                    (when (pos? row-count)
                      (util/with-close-on-catch [out-rel (Relation. allocator ^java.util.Map (update-keys vec-types str))]
                        (doseq [{:keys [write-row!]} out-rows]
                          (write-row! opts out-rel))

                        out-rel))))}))

(defn- emit-col-table [col-spec table-expr {:keys [schema] :as input-types}]
  (let [[out-col v] (first col-spec)
        expr (expr/form->expr v input-types)
        {:keys [vec-type ->list-expr]} (expr-list/compile-list-expr expr input-types)
        out-vec-types (-> {out-col vec-type}
                          (restrict-cols table-expr))
        out-type (get out-vec-types out-col)]
    {:vec-types out-vec-types
     :->out-rel (fn [{:keys [^BufferAllocator allocator, ^RelationReader args]}]
                  (util/with-close-on-catch [out-vec (Vector/open allocator (str out-col) out-type)]
                    (let [^ListExpression list-expr (->list-expr schema args)]
                      (when list-expr
                        (.writeTo list-expr out-vec 0 (.getSize list-expr)))
                      (Relation. allocator ^List (vector out-vec) (.getValueCount out-vec)))))}))

(defn- emit-arg-table [param table-expr {:keys [param-types]}]
  (let [vec-types (-> (into {} (for [^VectorType leg-type (or (get param-types param)
                                                               (throw (err/incorrect :unknown-table "Table refers to unknown param"
                                                                                     {:param param, :params (set (keys param-types))})))
                                     :when (not= #xt.arrow/type :null (.getArrowType leg-type))
                                     :when (or (= #xt.arrow/type :list (.getArrowType leg-type))
                                               (throw (err/incorrect :illegal-param-type "Table param must be of type struct list"
                                                                     {:param param})))
                                     :let [el-type (first (vals (.getChildren leg-type)))]
                                     ^VectorType el-leg-type el-type
                                     :when (or (= #xt.arrow/type :struct (.getArrowType el-leg-type))
                                               (= #xt.arrow/type :null (.getArrowType el-leg-type))
                                               (throw (err/incorrect :illegal-param-type "Table param must be of type struct list"
                                                                     {:param param})))
                                     [child-name ^VectorType child-type] (.getChildren el-leg-type)]
                                 (MapEntry/create (symbol child-name) child-type)))
                      (restrict-cols table-expr))]

    {:vec-types vec-types
     :->out-rel (fn [{:keys [^BufferAllocator allocator, ^RelationReader args]}]
                  (let [vec-rdr (.vectorForOrNull args (str (symbol param)))
                        list-rdr (cond-> vec-rdr
                                   (instance? ArrowType$Union (.getArrowType vec-rdr)) (.vectorFor "list"))
                        el-rdr (some-> list-rdr (.getListElements))
                        el-struct-rdr (cond-> el-rdr
                                              (instance? ArrowType$Union (.getArrowType el-rdr)) (.vectorFor "struct"))]

                    (Relation. allocator
                               ^List (vec (for [k (some-> el-struct-rdr .getKeyNames)
                                                :when (contains? vec-types (symbol k))]
                                            (.openSlice (.vectorFor el-struct-rdr k) allocator)))
                               (.getValueCount el-rdr))))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [vec-types ->out-rel row-count]} (zmatch table
                                                  [:rows rows] (emit-rows-table rows table-expr opts)
                                                  [:column col] (emit-col-table col table-expr opts)
                                                  [:param param] (emit-arg-table param table-expr opts))]

    {:op :table
     :children []
     :vec-types vec-types
     :stats (when row-count {:row-count row-count})
     :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span] :as opts}]
                 (cond-> (TableCursor. allocator (->out-rel opts))
                   (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))}))
