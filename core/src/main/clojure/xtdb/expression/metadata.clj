(ns xtdb.expression.metadata
  (:require [xtdb.expression :as expr]
            [xtdb.expression.walk :as ewalk]
            [xtdb.metadata :as meta]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import java.util.function.IntPredicate
           (xtdb.arrow RelationReader Vector VectorReader)
           (xtdb.arrow.metadata MetadataFlavour MetadataFlavour$Bytes MetadataFlavour$Numeric MetadataFlavour$Presence)
           (xtdb.bloom BloomUtils)
           (xtdb.metadata MetadataPredicate PageMetadata)))

(set! *unchecked-math* :warn-on-boxed)

(defn- simplify-and-or-expr [{:keys [op f args] :as expr}]
  (or (when (= op :call)
        (case f
          (:and :or) (let [args (->> args
                                     (into [] (comp (filter some?)
                                                    (remove (fn [{:keys [op] :as expr}]
                                                              (when (= :literal op)
                                                                (case f
                                                                  :and (true? (:literal expr))
                                                                  :or (false? (:literal expr))
                                                                  false)))))))]
                       (case (count args)
                         0 {:op :literal, :literal (case f :and true, :or false)}
                         1 (first args)
                         (-> expr (assoc :args (mapv simplify-and-or-expr args)))))
          nil))
      expr))

(defn- normalise-bool-args [[{x-op :op, :as x-expr} {y-op :op, :as y-expr}]]
  (case [x-op y-op]
    ([:param :param] [:literal :literal] [:literal :param] [:param :literal]) [:constant]
    ([:variable :literal] [:variable :param]) [:col-val x-expr y-expr]
    ([:literal :variable] [:param :variable]) [:val-col y-expr x-expr]

    nil))

(defn- minmax-expr [f min-or-max {col :variable} value-expr]
  (-> {:op :call, :f :or
       :args (for [value-type (-> (case (:op value-expr)
                                    :literal (vw/value->col-type (:literal value-expr))
                                    :param (:param-type value-expr))
                                  types/flatten-union-types)
                   :let [flavour-class (MetadataFlavour/getMetadataFlavour (types/->arrow-type value-type))]]
               (if (.isAssignableFrom MetadataFlavour$Numeric flavour-class)
                 {:op :test-minmax, :f f, :min-or-max min-or-max, :col col
                  :flavour-col (MetadataFlavour/getMetaColName flavour-class)

                  :value-expr value-expr
                  :double-sym (gensym 'meta-double)}
                 {:op :literal, :literal true}))}
      simplify-and-or-expr))

(defn- bloom-expr [{col :variable} value-expr]
  (-> {:op :call, :f :or
       :args (for [value-type (-> (case (:op value-expr)
                                    :literal (vw/value->col-type (:literal value-expr))
                                    :param (:param-type value-expr))
                                  types/flatten-union-types)]
               (if (= MetadataFlavour$Bytes (MetadataFlavour/getMetadataFlavour (types/->arrow-type value-type)))
                 {:op :test-bloom,
                  :col col,
                  :value-expr value-expr
                  :bloom-hash-sym (gensym 'bloom-hashes)}
                 {:op :literal, :literal true}))}

      simplify-and-or-expr))

(defn- presence-expr [{col :variable} value-expr]
  (-> {:op :call, :f :or
       :args (for [col-type (-> (case (:op value-expr)
                                  :literal (vw/value->col-type (:literal value-expr))
                                  :param (:param-type value-expr))
                                types/flatten-union-types)
                   :let [value-type (types/->arrow-type col-type)]]
               (if (= MetadataFlavour$Presence (MetadataFlavour/getMetadataFlavour value-type))
                 {:op :test-presence,
                  :col col,
                  :value-type value-type}

                 {:op :literal, :literal true}))}
      simplify-and-or-expr))

(declare meta-expr)

(defn call-meta-expr [{:keys [f args] :as expr} opts]
  (-> (or (case f
            :and {:op :call, :f :and, :args (map #(meta-expr % opts) args)}
            :or {:op :call, :f :or, :args (map #(meta-expr % opts) args)}

            (:< :<= :> :>= :=)
            (when-let [[field-val-tag col-expr val-expr] (normalise-bool-args args)]
              (case field-val-tag
                :constant expr
                :col-val (case f
                           :< (minmax-expr :< :min col-expr val-expr)
                           :<= (minmax-expr :<= :min col-expr val-expr)
                           :> (minmax-expr :> :max col-expr val-expr)
                           :>= (minmax-expr :>= :max col-expr val-expr)
                           := {:op :call, :f :and
                               :args [(minmax-expr :<= :min col-expr val-expr)
                                      (minmax-expr :>= :max col-expr val-expr)

                                      (bloom-expr col-expr val-expr)
                                      (presence-expr col-expr val-expr)]})

                :val-col (case f
                           :< (minmax-expr :> :max col-expr val-expr)
                           :<= (minmax-expr :>= :max col-expr val-expr)
                           :> (minmax-expr :< :min col-expr val-expr)
                           :>= (minmax-expr :<= :min col-expr val-expr)
                           := {:op :call, :f :and
                               :args [(minmax-expr :<= :min col-expr val-expr)
                                      (minmax-expr :>= :max col-expr val-expr)
                                      (bloom-expr col-expr val-expr)]})))

            nil)

          ;; we can't check this call at the metadata level, have to pull the block and look.
          {:op :literal, :literal true})

      simplify-and-or-expr))

(defn meta-expr [{:keys [op] :as expr} opts]
  (case op
    (:literal :param :let) nil ;; expected to be filtered out by the caller, using simplify-and-or-expr
    :variable {:op :literal, :literal true}
    :if (-> {:op :call
             :f :or
             :args [(meta-expr (:then expr) opts)
                    (meta-expr (:else expr) opts)]}
            simplify-and-or-expr)
    :call (call-meta-expr expr opts)))

(defn- ->bloom-hashes [allocator expr ^RelationReader params]
  (vec
   (for [{:keys [value-expr]} (->> (ewalk/expr-seq expr) (filter :bloom-hash-sym))]
     (case (:op value-expr)
       :literal (let [lit (:literal value-expr)]
                  (with-open [tmp-vec (Vector/fromField allocator (-> (vw/value->col-type lit) (types/col-type->field)))]
                    (.writeObject tmp-vec lit)
                    (BloomUtils/bloomHashes tmp-vec 0)))

       :param (BloomUtils/bloomHashes (.vectorFor params (str (:param value-expr))) 0)))))

(def ^:private table-metadata-sym (gensym "table-metadata"))
(def ^:private metadata-rdr-sym (gensym "metadata-rdr"))
(def ^:private col-rdr-sym (gensym "col-rdr"))
(def ^:private page-idx-sym (gensym "page-idx"))

(defmethod expr/codegen-expr :test-bloom [{:keys [col bloom-hash-sym]} _opts]
  (let [bloom-vec-sym (gensym "bloom-vec")]
    {:return-type :bool
     :batch-bindings [[bloom-vec-sym `(some-> (.vectorForOrNull ~col-rdr-sym "bytes")
                                              (.vectorForOrNull "bloom"))]]
     :continue (fn [cont]
                 (cont :bool
                       `(boolean
                         (let [~expr/idx-sym (.rowIndex ~table-metadata-sym ~(str col) ~page-idx-sym)]
                           (if (neg? ~expr/idx-sym)
                             false

                             ;; if this col is present but we haven't built blooms, we have to scan
                             (or (nil? ~bloom-vec-sym)
                                 (.isNull ~bloom-vec-sym ~expr/idx-sym)
                                 (BloomUtils/bloomContains ~bloom-vec-sym ~expr/idx-sym ~bloom-hash-sym)))))))}))

(defmethod expr/codegen-expr :test-presence [{:keys [col value-type]} _opts]
  (let [presence-vec (gensym 'presence)]
    {:return-type :bool
     :batch-bindings [[presence-vec `(.vectorForOrNull ~col-rdr-sym ~(types/arrow-type->leg value-type))]]
     :continue (fn [cont]
                 (cont :bool
                       `(boolean
                         (let [~expr/idx-sym (.rowIndex ~table-metadata-sym ~(str col) ~page-idx-sym)]
                           (and (not (neg? ~expr/idx-sym))
                                ~presence-vec
                                (not (.isNull ~presence-vec ~expr/idx-sym))
                                (.getBoolean ~presence-vec ~expr/idx-sym))))))}))

(defmethod expr/codegen-call [:_meta_double :num] [_expr]
  {:return-type :f64, :->call-code #(do `(double ~@%))})

(defmethod expr/codegen-call [:_meta_double :timestamp-tz] [{[[_ts-tz ts-unit _zone]] :arg-types}]
  {:return-type :f64, :->call-code #(do `(/ ~@% (double ~(types/ts-units-per-second ts-unit))))})

(defmethod expr/codegen-call [:_meta_double :timestamp-local] [{[[_ts-local ts-unit]] :arg-types}]
  {:return-type :f64, :->call-code #(do `(/ ~@% (double ~(types/ts-units-per-second ts-unit))))})

(defmethod expr/codegen-call [:_meta_double :date] [_]
  {:return-type :f64, :->call-code #(do `(* ~@% 86400.0))})

(defmethod expr/codegen-call [:_meta_double :time-local] [{[[_time-local ts-unit]] :arg-types}]
  {:return-type :f64, :->call-code #(do `(/ ~@% (double ~(types/ts-units-per-second ts-unit))))})

(defmethod expr/codegen-call [:_meta_double :duration] [{[[_duration ts-unit]] :arg-types}]
  {:return-type :f64, :->call-code #(do `(/ ~@% (double ~(types/ts-units-per-second ts-unit))))})

(defmethod expr/codegen-expr :test-minmax [{:keys [f min-or-max col flavour-col value-expr]} opts]
  (let [col-name (str col)
        col-sym (gensym 'meta_col)
        val-sym (gensym 'val)

        md-expr (expr/codegen-expr {:op :call, :f :_meta_double, :args [value-expr]} opts)]
    {:return-type :bool
     :children [md-expr]
     :batch-bindings [[(-> col-sym (expr/with-tag VectorReader))
                       `(some-> (.vectorForOrNull ~col-rdr-sym ~flavour-col)
                                (.vectorForOrNull ~(name min-or-max)))
                       val-sym ((:continue md-expr) (fn [_type code] code))]]
     :continue (fn [cont]
                 (cont :bool
                       `(boolean
                         (let [~expr/idx-sym (.rowIndex ~table-metadata-sym ~col-name ~page-idx-sym)]
                           (when (and ~col-sym (>= ~expr/idx-sym 0) (not (.isNull ~col-sym ~expr/idx-sym)))
                             (~(symbol f) (.getDouble ~col-sym ~expr/idx-sym) ~val-sym))))))}))

(defmethod ewalk/walk-expr :test-minmax [inner outer expr]
  (outer (-> expr (update :value-expr inner))))

(defmethod ewalk/direct-child-exprs :test-minmax [{:keys [value-expr]}] #{value-expr})

(def ^:private compile-meta-expr
  (-> (fn [expr opts]
        (let [expr (or (-> expr (expr/prepare-expr) (meta-expr opts) (expr/prepare-expr))
                       (expr/prepare-expr {:op :literal, :literal true}))
              {:keys [continue] :as emitted-expr} (expr/codegen-expr expr opts)]
          {:expr expr
           :f (-> `(fn [~(-> table-metadata-sym (expr/with-tag PageMetadata))
                        ~(-> expr/args-sym (expr/with-tag RelationReader))
                        [~@(keep :bloom-hash-sym (ewalk/expr-seq expr))]]
                     (let [~metadata-rdr-sym (.getMetadataLeafReader ~table-metadata-sym)
                           ~col-rdr-sym (-> (.vectorFor ~metadata-rdr-sym "columns")
                                            (.getListElements))

                           ~@(expr/batch-bindings emitted-expr)]
                       (reify IntPredicate
                         (~'test [_ ~page-idx-sym]
                           ~(continue (fn [_ code] code))))))
                  #_(doto clojure.pprint/pprint)
                  (eval))}))

      (util/lru-memoize)))

(defn ->metadata-selector ^xtdb.metadata.MetadataPredicate [allocator form col-types params]
  (let [param-types (expr/->param-types params)
        {:keys [expr f]} (compile-meta-expr (expr/form->expr form {:param-types param-types,
                                                                   :col-types col-types})
                                            {:param-types param-types
                                             :col-types col-types})
        bloom-hashes (->bloom-hashes allocator expr params)]
    (reify MetadataPredicate
      (build [_ table-metadata]
        (f table-metadata params bloom-hashes)))))
