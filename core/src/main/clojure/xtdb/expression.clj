(ns xtdb.expression
  (:require [xtdb.error :as err]
            [xtdb.expression.macro :as macro]
            [xtdb.expression.walk :as walk]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            xtdb.vector
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang Keyword MapEntry)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZoneOffset ZonedDateTime)
           (java.util Arrays Date List UUID)
           (java.util.regex Pattern)
           (java.util.stream IntStream)
           (org.apache.arrow.vector PeriodDuration ValueVector)
           (org.apache.commons.codec.binary Hex)
           (xtdb.operator IProjectionSpec IRelationSelector)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.util StringUtil)
           (xtdb.vector IMonoVectorReader IPolyVectorReader IStructValueReader IVectorReader MonoToPolyReader RelationReader RemappedTypeIdReader)
           xtdb.vector.ValueBox))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti parse-list-form
  (fn [[f & args] env]
    f)
  :default ::default)

(defn form->expr [form {:keys [col-types param-types locals] :as env}]
  (cond
    (symbol? form) (cond
                     (= 'xtdb/end-of-time form) {:op :literal, :literal util/end-of-time}
                     (contains? locals form) {:op :local, :local form}
                     (contains? param-types form) {:op :param, :param form, :param-type (get param-types form)}
                     (contains? col-types form) {:op :variable, :variable form, :var-type (get col-types form)}
                     :else (throw (err/illegal-arg :xtdb.expression/unknown-symbol
                                                   {::err/message (format "Unknown symbol: '%s'" form)
                                                    :symbol form})))

    (map? form) (do
                  (when-not (every? keyword? (keys form))
                    (throw (err/illegal-arg :xtdb.expression/parse-error
                                            {::err/message (str "keys to struct must be keywords: " (pr-str form))
                                             :form form})))
                  {:op :struct,
                   :entries (->> (for [[k v-form] form]
                                   (MapEntry/create k (form->expr v-form env)))
                                 (into {}))})

    (vector? form) {:op :list
                    :elements (mapv #(form->expr % env) form)}

    (seq? form) (parse-list-form form env)

    :else {:op :literal, :literal form}))

(defmethod parse-list-form 'if [[_ & args :as form] env]
  (when-not (= 3 (count args))
    (throw (err/illegal-arg :xtdb.expression/arity-error
                            {::err/message (str "'if' expects 3 args: " (pr-str form))
                             :form form})))

  (let [[pred then else] args]
    {:op :if,
     :pred (form->expr pred env),
     :then (form->expr then env),
     :else (form->expr else env)}))

(defmethod parse-list-form 'let [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (err/illegal-arg :xtdb.expression/arity-error
                            {::err/message (str "'let' expects 2 args - bindings + body"
                                                 (pr-str form))
                             :form form})))

  (let [[bindings body] args]
    (when-not (or (nil? bindings) (sequential? bindings))
      (throw (err/illegal-arg :xtdb.expression/parse-error
                              {::err/message (str "'let' expects a sequence of bindings: "
                                                   (pr-str form))
                               :form form})))

    (if-let [[local expr-form & more-bindings] (seq bindings)]
      (do
        (when-not (symbol? local)
          (throw (err/illegal-arg :xtdb.expression/parse-error
                                  {::err/message (str "bindings in `let` should be symbols: "
                                                       (pr-str local))
                                   :binding local})))
        {:op :let
         :local local
         :expr (form->expr expr-form env)
         :body (form->expr (list 'let more-bindings body)
                           (update env :locals (fnil conj #{}) local))})

      (form->expr body env))))

(defmethod parse-list-form '. [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (err/illegal-arg :xtdb.expression/arity-error
                            {::err/message (str "'.' expects 2 args: " (pr-str form))
                             :form form})))

  (let [[struct field] args]
    (when-not (or (symbol? field) (keyword? field))
      (throw (err/illegal-arg :xtdb.expression/arity-error
                              {::err/message (str "'.' expects symbol or keyword fields: " (pr-str form))
                               :form form})))

    {:op :call
     :f :get-field
     :args [(form->expr struct env)]
     :field (symbol field)}))

(defmethod parse-list-form '.. [[_ & args :as form] env]
  (let [[struct & fields] args]
    (when-not (seq fields)
      (throw (err/illegal-arg :xtdb.expression/arity-error
                              {::err/message (str "'..' expects at least 2 args: " (pr-str form))
                               :form form})))
    (when-not (every? #(or (symbol? %) (keyword? %)) fields)
      (throw (err/illegal-arg :xtdb.expression/parse-error
                              {::err/message (str "'..' expects symbol or keyword fields: " (pr-str form))
                               :form form})))
    (reduce (fn [struct-expr field]
              {:op :call
               :f :get-field
               :args [struct-expr]
               :field (symbol field)})
            (form->expr struct env)
            (rest args))))

(defmethod parse-list-form 'cast [[_ expr target-type] env]
  {:op :call
   :f :cast
   :args [(form->expr expr env)]
   :target-type target-type})

(defmethod parse-list-form ::default [[f & args] env]
  {:op :call
   :f (keyword (namespace f) (name f))
   :args (mapv #(form->expr % env) args)})

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(def type->cast
  (comp {:bool 'boolean
         :i8 'byte
         :i16 'short
         :i32 'int
         :i64 'long
         :f32 'float
         :f64 'double
         :timestamp-tz 'long
         :duration 'long
         :decimal 'bigdec}
        types/col-type-head))

(def idx-sym (gensym 'idx))
(def rel-sym (gensym 'rel))
(def params-sym (gensym 'params))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-expr
  "Returns a map containing
    * `:return-types` (set)
    * `:continue` (fn).
      Returned fn expects a function taking a single return-type and the emitted code for that return-type.
      May be called multiple times if there are multiple return types.
      Returned fn returns the emitted code for the expression (including the code supplied by the callback)."
  (fn [{:keys [op]} opts]
    op))

(defmulti codegen-cast
  (fn [{:keys [source-type target-type]}]
    [(types/col-type-head source-type) (types/col-type-head target-type)])
  :default ::default
  :hierarchy #'types/col-type-hierarchy)

(defmethod codegen-cast ::default [{:keys [source-type target-type]}]
  (if (= source-type target-type)
    {:return-type target-type
     :->call-code first}
    (throw (err/illegal-arg :xtdb.expression/cast-error
                            {::err/message (format "Unsupported cast: '%s' -> '%s'"
                                                   (pr-str source-type) (pr-str target-type))
                             :source-type source-type
                             :target-type target-type}))))

(defmethod codegen-cast [:num :num] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(~(type->cast target-type) ~@%))})

(defmethod codegen-cast [:num :utf8] [_]
  {:return-type :utf8, :->call-code #(do `(resolve-utf8-buf (str ~@%)))})

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti read-value-code
  (fn [col-type & args]
    (types/col-type-head col-type))
  :default ::default,
  :hierarchy #'types/col-type-hierarchy)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti write-value-code
  (fn [col-type & args]
    (types/col-type-head col-type))
  :default ::default,
  :hierarchy #'types/col-type-hierarchy)

(def ^:private col-type->rw-fn
  '{:bool Boolean, :i8 Byte, :i16 Short, :i32 Int, :i64 Long, :f32 Float, :f64 Double,
    :date Long, :time-local Long, :timestamp-tz Long, :timestamp-local Long, :duration Long
    :utf8 Bytes, :varbinary Bytes, :keyword Bytes, :uuid Bytes, :uri Bytes

    :list Object, :struct Object :clj-form Bytes})

(defmethod read-value-code :null [_ & _args] nil)
(defmethod write-value-code :null [_ w & _args] `(.writeNull ~w))

(doseq [k [:bool :i8 :i16 :i32 :i64 :f32 :f64
           :date :timestamp-tz :timestamp-local :time-local :duration
           :utf8 :varbinary :uuid :uri :keyword :clj-form]
        :let [rw-fn (col-type->rw-fn k)]]
  (defmethod read-value-code k [_ & args] `(~(symbol (str ".read" rw-fn)) ~@args))
  (defmethod write-value-code k [_ & args] `(~(symbol (str ".write" rw-fn)) ~@args)))

(doseq [[k tag] {:interval PeriodDuration :decimal BigDecimal}]
  (defmethod read-value-code k [_ & args]
    (-> `(.readObject ~@args) (with-tag tag)))

  (defmethod write-value-code k [_ & args] `(.writeObject ~@args)))

(defn- continue-read [f col-type reader-sym & args]
  (zmatch col-type
    [:union inner-types]
    `(case (.read ~reader-sym ~@args)
       ~@(->> inner-types
              (sequence
               (comp (map-indexed (fn [type-id col-type]
                                    [type-id (f col-type (read-value-code col-type reader-sym))]))
                     cat))))
    (f col-type (apply read-value-code col-type reader-sym args))))

(defmethod read-value-code :list [[_ el-type] & args]
  (-> `(.readObject ~@args)
      (with-tag (if (types/union? el-type) IPolyVectorReader IMonoVectorReader))))

(defmethod write-value-code :list [[_ list-el-type] writer-arg list-code]
  (let [list-sym (gensym 'list)
        n-sym (gensym 'n)
        el-writer-sym (gensym 'list-el-writer)]
    (if (types/union? list-el-type)
      `(let [~list-sym ~list-code
             el-count# (.valueCount ~list-sym)
             ~el-writer-sym (.listElementWriter ~writer-arg)]
         (.startList ~writer-arg)
         (dotimes [~n-sym el-count#]
           ~(continue-read (fn [el-type code]
                             (write-value-code el-type `(.legWriter ~el-writer-sym (types/->arrow-type '~el-type)) code))
                           list-el-type list-sym n-sym))
         (.endList ~writer-arg))

      `(let [~list-sym ~list-code
             el-count# (.valueCount ~list-sym)
             ~el-writer-sym (.listElementWriter ~writer-arg)]
         (.startList ~writer-arg)
         (dotimes [~n-sym el-count#]
           ~(continue-read (fn [el-type code]
                             (write-value-code el-type el-writer-sym code))
                           list-el-type list-sym n-sym))
         (.endList ~writer-arg)))))

(defmethod read-value-code :struct [_ & args]
  (-> `(.readObject ~@args)
      (with-tag IStructValueReader)))

(defmethod write-value-code :struct [[_ val-types] writer-arg code]
  (let [struct-sym (gensym 'struct)]
    `(let [~struct-sym ~code]
       (.startStruct ~writer-arg)
       ~@(for [[field val-type] val-types
               :let [field-name (str field)]]
           (if (types/union? val-type)
             (let [field-box-sym (gensym 'field_box)]
               `(let [~field-box-sym (.readField ~struct-sym ~field-name)]
                  ~(continue-read (fn [val-type code]
                                    (write-value-code val-type `(-> (.structKeyWriter ~writer-arg ~field-name)
                                                                    (.legWriter (types/->arrow-type '~val-type))) code))
                                  val-type field-box-sym)))

             (continue-read (fn [val-type code]
                              (write-value-code val-type `(.structKeyWriter ~writer-arg ~field-name) code))
                            val-type struct-sym field-name)))
       (.endStruct ~writer-arg))))

(def ^:dynamic ^java.time.Clock *clock* (Clock/systemUTC))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-value
  (fn [val-class code] val-class)
  :default ::default)

(defmethod emit-value ::default [_ code] code)
(defmethod emit-value Date [_ code] `(Math/multiplyExact (.getTime ~code) 1000))
(defmethod emit-value Instant [_ code] `(util/instant->micros ~code))
(defmethod emit-value ZonedDateTime [_ code] `(util/instant->micros (util/->instant ~code)))
(defmethod emit-value OffsetDateTime [_ code] `(util/instant->micros (util/->instant ~code)))
(defmethod emit-value LocalDateTime [_ code] `(util/instant->micros (.toInstant ~code ZoneOffset/UTC)))
(defmethod emit-value LocalTime [_ code] `(.toNanoOfDay ~code))
(defmethod emit-value Duration [_ code] `(quot (.toNanos ~code) 1000))
(defmethod emit-value UUID [_ code] `(util/uuid->byte-buffer ~code))

;; consider whether a bound hash map for literal parameters would be better
;; so this could be a runtime 'wrap the byte array' instead of this round trip through the clj compiler.
(defmethod emit-value (Class/forName "[B") [_ code] `(ByteBuffer/wrap (Hex/decodeHex (str ~(Hex/encodeHexString ^bytes code)))))

(defmethod emit-value String [_ code]
  `(-> (.getBytes ~code StandardCharsets/UTF_8) (ByteBuffer/wrap)))

(defmethod emit-value Keyword [_ code] (emit-value String `(str (symbol ~code))))

(defmethod emit-value LocalDate [_ code]
  ;; if literal, we can just yield a long directly
  (if (instance? LocalDate code)
    (.toEpochDay ^LocalDate code)
    `(.toEpochDay ~code)))

;; we emit these to PDs until the EE uses these types directly
(defmethod emit-value IntervalYearMonth [_ code]
  `(PeriodDuration. (.-period ~code) Duration/ZERO))

(defmethod emit-value IntervalDayTime [_ code]
  `(let [idt# ~code]
     (PeriodDuration. (.-period idt#) (.-duration idt#))))

(defmethod emit-value IntervalMonthDayNano [_ code]
  `(let [imdn# ~code]
     (PeriodDuration. (.-period imdn#) (.-duration imdn#))))

(defmethod codegen-expr :literal [{:keys [literal]} _]
  (let [return-type (vw/value->col-type literal)
        literal-type (class literal)]
    {:return-type return-type
     :continue (fn [f]
                 (f return-type (emit-value literal-type literal)))
     :literal literal}))

(defn lit->param [{:keys [op] :as expr}]
  (if (= op :literal)
    (let [{:keys [literal]} expr]
      {:op :param, :param (gensym 'lit),
       :param-type (vw/value->col-type literal)
       :literal literal})
    expr))

(defn prepare-expr [expr]
  (->> expr
       (macro/macroexpand-all)
       (walk/postwalk-expr lit->param)))

(defn- wrap-boxed-poly-return [{:keys [return-type continue] :as emitted-expr} _]
  (zmatch return-type
    [:union inner-types]
    (let [box-sym (gensym 'box)

          types (->> inner-types
                     (into {} (map-indexed (fn [idx val-type]
                                             (MapEntry/create val-type {:type-id (byte idx)
                                                                        :sym (symbol (str box-sym "w" idx))})))))]
      (-> emitted-expr
          (update :batch-bindings (fnil into []) (into [[box-sym `(ValueBox.)]]
                                                       (map (fn [{:keys [^long type-id sym]}]
                                                              [sym `(.writerForTypeId ~box-sym ~type-id)]))
                                                       (vals types)))
          (assoc :continue (fn [f]
                             `(do
                                ~(continue (fn [return-type code]
                                             (write-value-code return-type (:sym (get types return-type)) code)))

                                (case (.read ~box-sym)
                                  ~@(->> (for [[ret-type {:keys [type-id]}] types]
                                           [type-id (f ret-type (read-value-code ret-type box-sym))])
                                         (apply concat))))))))

    emitted-expr))

(defmethod codegen-expr :variable [{:keys [variable rel idx extract-vec-from-rel?],
                                    :or {rel rel-sym, idx idx-sym, extract-vec-from-rel? true}}
                                   {:keys [var->col-type extract-vecs-from-rel?]
                                    :or {extract-vecs-from-rel? true}}]
  ;; NOTE we now get the widest var-type in the expr itself, but don't use it here (yet? at all?)
  (let [col-type (or (get var->col-type variable)
                     (throw (AssertionError. (str "unknown variable: " variable))))
        sanitized-var (util/symbol->normal-form-symbol variable)]

    ;; NOTE: when used from metadata exprs, incoming vectors might not exist
    {:return-type col-type
     :batch-bindings (if (types/union? col-type)
                       (if (and extract-vecs-from-rel? extract-vec-from-rel?)
                         [[sanitized-var `(.polyReader (.readerForName ~rel ~(str variable))
                                                       '~col-type)]]
                         [[sanitized-var `(some-> ~sanitized-var (.polyReader '~col-type))]])

                       (if (and extract-vecs-from-rel? extract-vec-from-rel?)
                         [[sanitized-var `(.monoReader (.readerForName ~rel ~(str variable)) '~col-type)]]
                         [[sanitized-var `(some-> ~sanitized-var (.monoReader '~col-type))]]))
     :continue (fn [f]
                 (continue-read f col-type sanitized-var idx))}))

(defmethod codegen-expr :param [{:keys [param] :as expr} {:keys [param-types]}]
  (if-let [[_ literal] (find expr :literal)]
    (let [lit-type (vw/value->col-type literal)
          lit-class (class literal)]
      (into {:return-type lit-type
             :batch-bindings [[param (emit-value lit-class literal)]]
             :continue (fn [f]
                         (f lit-type param))}
            (select-keys expr #{:literal})))

    (codegen-expr {:op :variable, :variable param, :rel params-sym, :idx 0}
                  {:var->col-type {param (get param-types param)}})))

(defmethod codegen-expr :if [{:keys [pred then else]} opts]
  (let [{p-cont :continue, :as emitted-p} (codegen-expr pred opts)
        {t-ret :return-type, t-cont :continue, :as emitted-t} (codegen-expr then opts)
        {e-ret :return-type, e-cont :continue, :as emitted-e} (codegen-expr else opts)
        return-type (types/merge-col-types t-ret e-ret)]
    (-> {:return-type return-type
         :children [emitted-p emitted-t emitted-e]
         :continue (fn [f]
                     `(if ~(p-cont (fn [_ code] `(boolean ~code)))
                        ~(t-cont f)
                        ~(e-cont f)))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :local [{:keys [local]} {:keys [local-types]}]
  (let [return-type (get local-types local)]
    {:return-type return-type
     :continue (fn [f] (f return-type local))}))

(defmethod codegen-expr :let [{:keys [local expr body]} opts]
  (let [{local-type :return-type, continue-expr :continue, :as emitted-expr} (codegen-expr expr opts)
        emitted-bodies (->> (for [local-type (types/flatten-union-types local-type)]
                              (MapEntry/create local-type
                                               (codegen-expr body (assoc-in opts [:local-types local] local-type))))
                            (into {}))
        return-type (apply types/merge-col-types (into #{} (map :return-type) (vals emitted-bodies)))]
    (-> {:return-type return-type
         :children (cons emitted-expr (vals emitted-bodies))
         :continue (fn [f]
                     (continue-expr (fn [local-type code]
                                      `(let [~local ~code]
                                         ~((:continue (get emitted-bodies local-type)) f)))))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :if-some [{:keys [local expr then else]} opts]
  (let [{continue-expr :continue, expr-ret :return-type, :as emitted-expr}
        (codegen-expr expr opts)

        expr-rets (types/flatten-union-types expr-ret)

        emitted-thens (->> (for [local-type (disj expr-rets :null)]
                             (MapEntry/create local-type
                                              (codegen-expr then (assoc-in opts [:local-types local] local-type))))
                           (into {}))
        then-rets (into #{} (map :return-type) (vals emitted-thens))
        then-ret (apply types/merge-col-types then-rets)

        children (cons emitted-expr (vals emitted-thens))]

    (-> (if-not (contains? expr-rets :null)
          {:return-type then-ret
           :children children
           :continue (fn [f]
                       (continue-expr (fn [local-type code]
                                        `(let [~local ~code]
                                           ~((:continue (get emitted-thens local-type)) f)))))}

          (let [{e-ret :return-type, e-cont :continue, :as emitted-else} (codegen-expr else opts)
                return-type (apply types/merge-col-types e-ret then-rets)]
            {:return-type return-type
             :children (cons emitted-else children)
             :continue (fn [f]
                         (continue-expr (fn [local-type code]
                                          (if (= local-type :null)
                                            (e-cont f)
                                            `(let [~local ~code]
                                               ~((:continue (get emitted-thens local-type)) f))))))}))
        (wrap-boxed-poly-return opts))))

(defmulti codegen-call
  "Expects a map containing both the expression and an `:arg-types` key - a vector of col-types.
   This `:arg-types` vector should be monomorphic - if the args are polymorphic, call this multimethod multiple times.

   Returns a map containing
    * `:return-type`
    and one of the following
    * for monomorphic return types: `:->call-code` :: emitted-args -> code
    * for polymorphic return types: `:continue-call` :: f, emitted-args -> code
        where f :: return-type, code -> code"

  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (namespace f) (name f))
               (map types/col-type-head arg-types))))

  :hierarchy #'types/col-type-hierarchy)

(def ^:private shortcut-null-args?
  (complement (comp #{:true? :false? :nil? :boolean :null-eq
                      :compare-nulls-first :compare-nulls-last
                      :period}
                    keyword)))

(defn- cont-b3-call [arg-type code]
  (if (= :null arg-type)
    0
    `(long (if ~code 1 -1))))

(defn- nullable? [col-type]
  (contains? (types/flatten-union-types col-type) :null))

(defn- codegen-and [[{l-ret :return-type, l-cont :continue}
                     {r-ret :return-type, r-cont :continue}]]
  (let [nullable? (or (nullable? l-ret) (nullable? r-ret))]
    {:return-type (if nullable? [:union #{:bool :null}] :bool)
     :continue (if-not nullable?
                 (fn [f]
                   (f :bool `(and ~(l-cont (fn [_ code] `(boolean ~code)))
                                  ~(r-cont (fn [_ code] `(boolean ~code))))))

                 (let [l-sym (gensym 'l)
                       r-sym (gensym 'r)]
                   (fn [f]
                     `(let [~l-sym ~(l-cont cont-b3-call)
                            ~r-sym (long (if (neg? ~l-sym)
                                           -1
                                           ~(r-cont cont-b3-call)))]
                        (if (zero? (min ~l-sym ~r-sym))
                          ~(f :null nil)
                          ~(f :bool `(== 1 ~l-sym ~r-sym)))))))}))

(defn- codegen-or [[{l-ret :return-type, l-cont :continue}
                    {r-ret :return-type, r-cont :continue}]]
  (let [nullable? (or (nullable? l-ret) (nullable? r-ret))]
    {:return-type (if nullable? [:union #{:bool :null}] :bool)
     :continue (if-not nullable?
                 (fn [f]
                   (f :bool `(or ~(l-cont (fn [_ code] `(boolean ~code)))
                                 ~(r-cont (fn [_ code] `(boolean ~code))))))

                 (let [l-sym (gensym 'l)
                       r-sym (gensym 'r)]
                   (fn [f]
                     `(let [~l-sym ~(l-cont cont-b3-call)
                            ~r-sym (long (if (pos? ~l-sym)
                                           1
                                           ~(r-cont cont-b3-call)))]
                        (if (zero? (max ~l-sym ~r-sym))
                          ~(f :null nil)
                          ~(f :bool `(not (== -1 ~l-sym ~r-sym))))))))}))

(defn- codegen-call* [{:keys [f emitted-args] :as expr}]
  (let [shortcut-null? (shortcut-null-args? f)

        all-arg-types (reduce (fn [acc {:keys [return-type]}]
                                (for [el acc
                                      return-type (types/flatten-union-types return-type)]
                                  (conj el return-type)))
                              [[]]
                              emitted-args)

        emitted-calls (->> (for [arg-types all-arg-types
                                 :when (or (not shortcut-null?)
                                           (every? #(not= :null %) arg-types))]
                             (MapEntry/create arg-types
                                              (codegen-call (assoc expr
                                                                   :args emitted-args
                                                                   :arg-types arg-types))))
                           (into {}))

        return-type (->> (cond-> (into #{} (map :return-type) (vals emitted-calls))
                           (and shortcut-null?
                                (some #(= :null %)
                                      (sequence cat all-arg-types)))
                           (conj :null))
                         (apply types/merge-col-types))]

    {:return-type return-type
     :batch-bindings (->> (vals emitted-calls) (into [] (mapcat :batch-bindings)))
     :continue (fn continue-call-expr [handle-emitted-expr]
                 (let [build-args-then-call
                       (reduce (fn step [build-next-arg {continue-this-arg :continue}]
                                 ;; step: emit this arg, and pass it through to the inner build-fn
                                 (fn continue-building-args [arg-types emitted-args]
                                   (continue-this-arg (fn [arg-type emitted-arg]
                                                        (if (and shortcut-null? (= arg-type :null))
                                                          (handle-emitted-expr :null nil)
                                                          (build-next-arg (conj arg-types arg-type)
                                                                          (conj emitted-args emitted-arg)))))))

                               ;; innermost fn - we're done, call continue-call for these types
                               (fn call-with-built-args [arg-types emitted-args]
                                 (let [{:keys [return-type continue-call ->call-code]} (get emitted-calls arg-types)]
                                   (cond
                                     continue-call (continue-call handle-emitted-expr emitted-args)
                                     ->call-code (handle-emitted-expr return-type (->call-code emitted-args))
                                     :else (throw (IllegalStateException.
                                                   (str "internal error: invalid call definition"
                                                        (pr-str {:f f, :arg-types arg-types})))))))

                               ;; reverse because we're working inside-out
                               (reverse emitted-args))]
                   (build-args-then-call [] [])))}))

(declare codegen-concat)

(defmethod codegen-expr :call [{:keys [f args] :as expr} opts]
  (let [emitted-args (for [arg args]
                       (codegen-expr arg opts))
        expr (assoc expr :emitted-args emitted-args)]
    (-> (case f
          :and (codegen-and emitted-args)
          :or (codegen-or emitted-args)
          :concat (codegen-concat expr)
          (codegen-call* expr))

        (assoc :children emitted-args)
        (wrap-boxed-poly-return opts))))

(doseq [[f-kw cmp] [[:< #(do `(neg? ~%))]
                    [:<= #(do `(not (pos? ~%)))]
                    [:> #(do `(pos? ~%))]
                    [:>= #(do `(not (neg? ~%)))]]
        :let [f-sym (symbol (name f-kw))]]

  (doseq [col-type #{:num :timestamp-tz :duration :date}]
    (defmethod codegen-call [f-kw col-type col-type] [_]
      {:return-type :bool, :->call-code #(do `(~f-sym ~@%))}))

  (doseq [col-type #{:varbinary :utf8 :keyword :uuid :uri}]
    (defmethod codegen-call [f-kw col-type col-type] [_]
      {:return-type :bool, :->call-code #(cmp `(util/compare-nio-buffers-unsigned ~@%))}))

  (defmethod codegen-call [f-kw :any :any] [_]
    {:return-type :bool, :->call-code #(cmp `(compare ~@%))}))

(defmethod codegen-call [:= :num :num] [_]
  {:return-type :bool
   :->call-code #(do `(== ~@%))})

(defmethod codegen-call [:= :any :any] [_]
  {:return-type :bool
   :->call-code #(do `(= ~@%))})

(doseq [col-type #{:varbinary :utf8 :uuid}]
  (defmethod codegen-call [:= col-type col-type] [_]
    {:return-type :bool,
     :->call-code #(do `(.equals ~@%))}))

;; null-eq is an internal function used in situations where two nulls should compare equal,
;; e.g when grouping rows in group-by.
(defmethod codegen-call [:null-eq :any :any] [call]
  (codegen-call (assoc call :f :=)))

(defmethod codegen-call [:null-eq :null :any] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-call [:null-eq :any :null] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-call [:null-eq :null :null] [_]
  {:return-type :bool, :->call-code (constantly true)})

(defmethod codegen-call [:<> :num :num] [_]
  {:return-type :bool, :->call-code #(do `(not (== ~@%)))})

(defmethod codegen-call [:<> :any :any] [_]
  {:return-type :bool, :->call-code #(do `(not= ~@%))})

(defmethod codegen-call [:not :bool] [_]
  {:return-type :bool, :->call-code #(do `(not ~@%))})

(defmethod codegen-call [:true? :bool] [_]
  {:return-type :bool, :->call-code #(do `(true? ~@%))})

(defmethod codegen-call [:false? :bool] [_]
  {:return-type :bool, :->call-code #(do `(false? ~@%))})

(defmethod codegen-call [:nil? :null] [_]
  {:return-type :bool, :->call-code (constantly true)})

(doseq [f #{:true? :false? :nil?}]
  (defmethod codegen-call [f :any] [_]
    {:return-type :bool, :->call-code (constantly false)}))

(defmethod codegen-call [:boolean :null] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-call [:boolean :bool] [_]
  {:return-type :bool, :->call-code first})

(defmethod codegen-call [:boolean :any] [_]
  {:return-type :bool, :->call-code (constantly true)})

(defn- with-math-integer-cast
  "java.lang.Math's functions only take int or long, so we introduce an up-cast if need be"
  [int-type emitted-args]
  (let [arg-cast (if (isa? types/widening-hierarchy int-type :i32) 'int 'long)]
    (map #(list arg-cast %) emitted-args)))

(defmethod codegen-call [:+ :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/addExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:+ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(+ ~@%))})

(defmethod codegen-call [:- :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:- :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(- ~@%))})

(defmethod codegen-call [:- :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  (list (type->cast x-type)
                        `(Math/negateExact ~@(with-math-integer-cast x-type emitted-args))))})

(defmethod codegen-call [:- :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code #(do `(- ~@%))})

(defmethod codegen-call [:* :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/multiplyExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:* :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(* ~@%))})

(defmethod codegen-call [:mod :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(mod ~@%))})

(defn throw-div-0 []
  (throw (err/runtime-err ::division-by-zero
                          {::err/message "data exception — division by zero"})))

(defmethod codegen-call [:/ :int :int] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(try
                        (quot ~@%)
                        (catch ArithmeticException e#
                          (case (.getMessage e#)
                            "/ by zero" (throw-div-0)
                            (throw e#)))))})

(defmethod codegen-call [:/ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code (fn [[l r]]
                  `(let [l# ~l, r# ~r]
                     (if (zero? r#)
                       (throw-div-0)
                       (/ l# r#))))})

;; TODO extend min/max to variable width
(defmethod codegen-call [:greatest :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(Math/max ~@%))})

(defmethod codegen-call [:least :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(Math/min ~@%))})

(defmethod codegen-call [:power :num :num] [_]
  {:return-type :f64, :->call-code #(do `(Math/pow ~@%))})

(defmethod codegen-call [:log :num :num] [_]
  {:return-type :f64,
   :->call-code (fn [[base x]]
                  `(/ (Math/log ~x) (Math/log ~base)))})

(defmethod codegen-call [:double :num] [_]
  {:return-type :f64, :->call-code #(do `(double ~@%))})

(defn like->regex [like-pattern]
  (-> like-pattern
      (Pattern/quote)
      (.replace "%" "\\E.*\\Q")
      (.replace "_" "\\E.\\Q")
      (->> (format "^%s\\z"))
      re-pattern))

(defmethod codegen-call [:like :utf8 :utf8] [{[_ {:keys [literal]}] :args}]
  {:return-type :bool
   :->call-code (fn [[haystack-code needle-code]]
                  `(boolean (re-find ~(if literal
                                        (like->regex literal)
                                        `(like->regex (resolve-string ~needle-code)))
                                     (resolve-string ~haystack-code))))})

(defn binary->hex-like-pattern
  "Returns a like pattern that will match on binary encoded to hex.

  Percent/underscore are encoded as the ascii/utf-8 binary value for their respective characters i.e

  The byte 95 should be used for _
  The byte 37 should be used for %.

  Useful for now to use the string-impl of LIKE for binary search too."
  [^bytes bin]
  (-> (Hex/encodeHexString bin)
      (.replace "25" "%")
      ;; we are matching two chars per byte (hex)
      (.replace "5f" "__")))

(defn buf->bytes
  "Returns a byte array given an nio buffer. Gives you the backing array if possible, else allocates a new array."
  ^bytes [^ByteBuffer o]
  (if (.hasArray o)
    (.array o)
    (let [size (.remaining o)
          arr (byte-array size)]
      (.get o arr)
      arr)))

(defn resolve-bytes
  "Values of type ArrowType$Binary may have differing literal representations, e.g a byte array, or NIO byte buffer.

  Use this function when you are unsure of the representation at hand and just want a byte array."
  ^bytes [binary]
  (if (bytes? binary)
    binary
    (buf->bytes binary)))

(defn resolve-buf
  "Values of type ArrowType$Binary may have differing literal representations, e.g a byte array, or NIO byte buffer.

  Use this function when you are unsure of the representation at hand and just want a ByteBuffer."
  ^ByteBuffer [buf-or-bytes]
  (if (instance? ByteBuffer buf-or-bytes)
    buf-or-bytes
    (ByteBuffer/wrap ^bytes buf-or-bytes)))

(defmethod codegen-call [:like :varbinary :varbinary] [{[_ {:keys [literal]}] :args}]
  {:return-type :bool
   :->call-code (fn [[haystack-code needle-code]]
                  `(boolean (re-find ~(if literal
                                        (like->regex (binary->hex-like-pattern (resolve-bytes literal)))
                                        `(like->regex (binary->hex-like-pattern (buf->bytes ~needle-code))))
                                     (Hex/encodeHexString (buf->bytes ~haystack-code)))))})

(defmethod codegen-call [:like-regex :utf8 :utf8 :utf8] [{:keys [args]}]
  (let [[_ re-literal flags] (map :literal args)

        flag-map
        {\s [Pattern/DOTALL]
         \i [Pattern/CASE_INSENSITIVE, Pattern/UNICODE_CASE]
         \m [Pattern/MULTILINE]}

        flag-int (int (reduce bit-or 0 (mapcat flag-map flags)))]

    {:return-type :bool
     :->call-code (fn [[haystack-code needle-code]]
                    `(boolean (re-find ~(if re-literal
                                          (Pattern/compile re-literal flag-int)
                                          `(Pattern/compile (resolve-string ~needle-code) ~flag-int))
                                       (resolve-string ~haystack-code))))}))

;;;; SQL Trim functions.
;; trim-char is a **SINGLE** unicode-character string e.g \" \".
;; The string can include 2-char code points, as long as it is one logical unicode character.

;; N.B trim-char is currently restricted to just one character, not all database implementations behave this way.
;; in postgres you can specify multi-character strings for the trim char.

;; Apache Commons has these functions but did not think they were worth the dep.
;; replace if we ever put commons on classpath.

(defn- assert-trim-char-conforms
  "Trim char is allowed to be length 1, or 2 - but only 2 if the 2 chars represent a single unicode character
  (but are split due to utf-16 surrogacy)."
  [^String trim-char]
  (when-not (or (= 1 (.length trim-char))
                (and (= 2 (.length trim-char))
                     (= 2 (Character/charCount (.codePointAt trim-char 0)))))
    (throw (err/runtime-err :xtdb.expression/data-exception
                            {::err/message "Data Exception - trim error."}))))

(defn sql-trim-leading ^String [^String s, ^String trim-char]
  (assert-trim-char-conforms trim-char)

  (let [trim-cp (.codePointAt trim-char 0)]
    (loop [i 0]
      (if (< i (.length s))
        (let [cp (.codePointAt s i)]
          (if (= cp trim-cp)
            (recur (unchecked-add-int i (Character/charCount cp)))
            (.substring s i (.length s))))
        ""))))

(defn sql-trim-trailing ^String [^String s, ^String trim-char]
  (assert-trim-char-conforms trim-char)

  (let [trim-cp (.codePointAt trim-char 0)]
    (loop [i (unchecked-dec-int (.length s))
           len (.length s)]
      (if (< -1 i)
        (let [cp (.codePointAt s i)]
          (if (= cp trim-cp)
            (recur (unchecked-subtract-int i (Character/charCount cp))
                   (unchecked-subtract-int len (Character/charCount cp)))
            (.substring s 0 len)))
        ""))))

(defn sql-trim-both ^String [^String s, ^String trim-char]
  (-> s
      (sql-trim-leading trim-char)
      (sql-trim-trailing trim-char)))

(defmethod codegen-call [:trim-leading :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s trim-char]]
                  `(-> (sql-trim-leading (resolve-string ~s) (resolve-string ~trim-char))
                       (.getBytes StandardCharsets/UTF_8)
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim-trailing :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s trim-char]]
                  `(-> (sql-trim-trailing (resolve-string ~s) (resolve-string ~trim-char))
                       (.getBytes StandardCharsets/UTF_8)
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s trim-char]]
                  `(-> (sql-trim-both (resolve-string ~s) (resolve-string ~trim-char))
                       (.getBytes StandardCharsets/UTF_8)
                       ByteBuffer/wrap))})

;;;; SQL Trim function on binary.
;; trim-octet is the byte value to trim.

(defn binary-trim-leading ^bytes [^bytes bin, trim-octet]
  (let [trim-octet (byte trim-octet)]
    (loop [i 0]
      (if (< i (alength bin))
        (let [v (aget bin i)]
          (if (= v trim-octet)
            (recur (unchecked-inc-int i))
            (if (zero? i)
              bin
              (Arrays/copyOfRange bin i (alength bin)))))
        (byte-array 0)))))

(defn binary-trim-trailing ^bytes [^bytes bin trim-octet]
  (let [trim-octet (byte trim-octet)]
    (loop [i (unchecked-dec-int (alength bin))
           len (alength bin)]
      (if (< -1 i)
        (let [v (aget bin i)]
          (if (= v trim-octet)
            (recur (unchecked-dec-int i) (unchecked-dec-int len))
            (if (= len (alength bin))
              bin
              (Arrays/copyOfRange bin 0 len))))
        (byte-array 0)))))

(defn binary-trim-both ^bytes [^bytes bin, trim-octet]
  (-> bin
      (binary-trim-leading trim-octet)
      (binary-trim-trailing trim-octet)))

(defmethod codegen-call [:trim-leading :varbinary :varbinary] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  `(-> (resolve-bytes ~s)
                       (binary-trim-leading (first (resolve-bytes ~trim-octet)))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim-trailing :varbinary :varbinary] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  `(-> (resolve-bytes ~s)
                       (binary-trim-trailing (first (resolve-bytes ~trim-octet)))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim :varbinary :varbinary] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  `(-> (resolve-bytes ~s)
                       (binary-trim-both (first (resolve-bytes ~trim-octet)))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim-leading :varbinary :num] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  ;; should we throw an explicit error if no good cast to byte is possible?
                  `(-> (resolve-bytes ~s)
                       (binary-trim-leading (byte ~trim-octet))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim-trailing :varbinary :num] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  ;; should we throw an explicit error if no good cast to byte is possible?
                  `(-> (resolve-bytes ~s)
                       (binary-trim-trailing (byte ~trim-octet))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim :varbinary :num] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  ;; should we throw an explicit error if no good cast to byte is possible?
                  `(-> (resolve-bytes ~s)
                       (binary-trim-both (byte ~trim-octet))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:upper :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[code]]
                  `(ByteBuffer/wrap (.getBytes (.toUpperCase (resolve-string ~code)) StandardCharsets/UTF_8)))})

(defmethod codegen-call [:lower :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[code]]
                  `(ByteBuffer/wrap (.getBytes (.toLowerCase (resolve-string ~code)) StandardCharsets/UTF_8)))})

(defn- allocate-concat-out-buffer ^ByteBuffer [bufs]
  (loop [i (int 0)
         capacity (int 0)]
    (if (< i (count bufs))
      (let [^ByteBuffer buf (nth bufs i)]
        (recur (unchecked-inc-int i) (Math/addExact (int capacity) (.remaining buf))))
      (ByteBuffer/allocate capacity))))

(defn buf-concat
  "Concatenates multiple byte buffers, to implement variadic string / binary concat."
  ^ByteBuffer [bufs]

  (let [out (allocate-concat-out-buffer bufs)]
    (doseq [^ByteBuffer buf bufs]
      (let [pos (.position buf)]
        (.put out buf)
        (.position buf pos)))

    (doto out
      (.position 0))))

(defn resolve-utf8-buf ^ByteBuffer [s-or-buf]
  (if (instance? ByteBuffer s-or-buf)
    s-or-buf
    (ByteBuffer/wrap (.getBytes ^String s-or-buf StandardCharsets/UTF_8))))

;; concat is not a simple mono-call, as it permits a variable number of arguments so we can avoid intermediate alloc
(defn- codegen-concat [{:keys [emitted-args] :as expr}]
  (when (< (count emitted-args) 2)
    (throw (err/illegal-arg :xtdb.expression/arity-error
                            {::err/message "Arity error, concat requires at least two arguments"
                             :expr (dissoc expr :emitted-args :arg-types)})))
  (let [possible-types (into #{} (mapcat (comp types/flatten-union-types :return-type)) emitted-args)
        value-types (disj possible-types :null)
        _ (when (< 1 (count value-types))
            (throw (err/illegal-arg :xtdb.expression/type-error
                                    {::err/message "All arguments to `concat` must be of the same type."
                                     :types value-types})))
        value-type (first value-types)
        ->concat-code (case value-type
                        :utf8 #(do `(buf-concat (mapv resolve-utf8-buf [~@%])))
                        :varbinary #(do `(buf-concat (mapv resolve-buf [~@%])))
                        nil nil)]
    {:return-type (apply types/merge-col-types possible-types)
     :continue (fn continue-call-expr [handle-emitted-expr]
                 (let [build-args-then-call
                       (reduce (fn step [build-next-arg {continue-this-arg :continue}]
                                 (fn continue-building-args [emitted-args]
                                   (continue-this-arg (fn [arg-type emitted-arg]
                                                        (if (= arg-type :null)
                                                          (handle-emitted-expr :null nil)
                                                          (build-next-arg (conj emitted-args emitted-arg)))))))
                               (fn call-with-built-args [emitted-args]
                                 (handle-emitted-expr value-type (->concat-code emitted-args)))
                               (reverse emitted-args))]
                   (build-args-then-call [])))}))

(defmethod codegen-call [:substring :utf8 :int] [_]
  {:return-type :utf8
   :->call-code (fn [[x start]]
                  `(StringUtil/sqlUtf8Substring (resolve-utf8-buf ~x) ~start -1 false))})

(defmethod codegen-call [:substring :utf8 :int :int] [_]
  {:return-type :utf8
   :->call-code (fn [[x start length]]
                  `(StringUtil/sqlUtf8Substring (resolve-utf8-buf ~x) ~start ~length true))})

(defmethod codegen-call [:substring :varbinary :int] [_]
  {:return-type :varbinary
   :->call-code (fn [[x start]]
                  `(StringUtil/sqlBinSubstring (resolve-buf ~x) ~start -1 false))})

(defmethod codegen-call [:substring :varbinary :int :int] [_]
  {:return-type :varbinary
   :->call-code (fn [[x start length]]
                  `(StringUtil/sqlBinSubstring (resolve-buf ~x) ~start ~length true))})

(defmethod codegen-call [:character-length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(StringUtil/utf8Length (resolve-utf8-buf ~(first %))))})

(defmethod codegen-call [:octet-length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-utf8-buf ~@%)))})

(defmethod codegen-call [:octet-length :varbinary] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-buf ~@%)))})

(defmethod codegen-call [:position :utf8 :utf8] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/sqlUtf8Position (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))})

(defmethod codegen-call [:octet-position :utf8 :utf8] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/SqlBinPosition (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))})

(defmethod codegen-call [:octet-position :varbinary :varbinary] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/SqlBinPosition (resolve-buf ~needle) (resolve-buf ~haystack)))})

(defmethod codegen-call [:overlay :utf8 :utf8 :int :int] [_]
  {:return-type :utf8
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlUtf8Overlay (resolve-utf8-buf ~target) (resolve-utf8-buf ~replacement) ~from ~len))})

(defmethod codegen-call [:overlay :varbinary :varbinary :int :int] [_]
  {:return-type :varbinary
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlBinOverlay (resolve-buf ~target) (resolve-buf ~replacement) ~from ~len))})

(defmethod codegen-call [:default-overlay-length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(StringUtil/utf8Length (resolve-utf8-buf ~@%)))})

(defmethod codegen-call [:default-overlay-length :varbinary] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-buf ~@%)))})

(doseq [[math-op math-method] {:sin 'Math/sin, :cos 'Math/cos, :tan 'Math/tan
                               :sinh 'Math/sinh, :cosh 'Math/cosh, :tanh 'Math/tanh
                               :asin 'Math/asin, :acos 'Math/acos, :atan 'Math/atan
                               :sqrt 'Math/sqrt, :ln 'Math/log, :log10 'Math/log10, :exp 'Math/exp
                               :floor 'Math/floor, :ceil 'Math/ceil}]
  (defmethod codegen-call [math-op :num] [_]
    {:return-type :f64
     :->call-code #(do `(~math-method ~@%))}))

(defn buf->str ^String [^ByteBuffer buf]
  (let [bs (byte-array (.remaining buf))
        pos (.position buf)]
    (.get buf bs)
    (.position buf pos)
    (String. bs)))

(doseq [[col-type parse-sym] [[:i8 `Byte/parseByte]
                              [:i16 `Short/parseShort]
                              [:i32 `Integer/parseInt]
                              [:i64 `Long/parseLong]
                              [:f32 `Float/parseFloat]
                              [:f64 `Double/parseDouble]]]
  (defmethod codegen-cast [:utf8 col-type] [_]
    {:return-type col-type, :->call-code #(do `(~parse-sym (buf->str ~@%)))}))

(defmethod codegen-call [:cast :any] [{[source-type] :arg-types, :keys [target-type]}]
  (codegen-cast {:source-type source-type, :target-type target-type}))

(defmethod codegen-expr :struct [{:keys [entries]} opts]
  (let [emitted-vals (->> entries
                          (into {} (map (juxt (comp symbol key) (comp #(codegen-expr % opts) val)))))
        val-types (->> emitted-vals
                       (into {} (map (juxt key (comp :return-type val)))))

        {poly-vals true, mono-vals false}
        (->> emitted-vals
             (group-by (comp types/union? :return-type val)))

        return-type [:struct val-types]
        box-sym (when (seq poly-vals)
                  (gensym 'box))]

    {:return-type return-type
     :batch-bindings (cond-> [] box-sym (conj [box-sym `(ValueBox.)]))
     :children (vals emitted-vals)
     :continue (fn [f]
                 (f return-type
                    `(reify IStructValueReader
                       ~@(when (seq poly-vals)
                           [`(~'readField [_# field#]
                              (case field#
                                ~@(->> poly-vals
                                       (mapcat (fn [[field {:keys [return-type continue]}]]
                                                 (let [type-ids (->> (second return-type)
                                                                     (into {} (map-indexed (fn [idx val-type]
                                                                                             [val-type idx]))))]
                                                   [(str field)
                                                    (continue (fn [val-type code]
                                                                (write-value-code val-type `(.writerForTypeId ~box-sym ~(get type-ids val-type)) code)))])))))
                              ~box-sym)])

                       ~@(for [[rw-fn mono-vals] (->> mono-vals
                                                      (group-by (comp col-type->rw-fn types/col-type-head :return-type val)))
                               :when rw-fn]
                           `(~(symbol (str "read" rw-fn)) [_# field#]
                             (case field#
                               ~@(->> (for [[field {:keys [continue]}] mono-vals]
                                        [(str field)
                                         (continue (fn [_ code] code))])
                                      (apply concat))))))))}))

(defmethod codegen-call [:get-field :struct] [{[[_ field-types]] :arg-types, :keys [field]}]
  (if-let [val-type (get field-types field)]
    {:return-type val-type
     :continue-call (fn [f [struct-code]]
                      (if (types/union? val-type)
                        (continue-read f val-type `(.readField ~struct-code ~(str field)))
                        (continue-read f val-type struct-code (str field))))}

    {:return-type :null, :->call-code (constantly nil)}))

(defmethod codegen-call [:get-field :any] [_]
  {:return-type :null, :->call-code (constantly nil)})

(defmethod codegen-call [:= :struct :struct] [{[[_ l-field-types] [_ r-field-types]] :arg-types}]
  (let [fields (set (keys l-field-types))]
    (if-not (= fields (set (keys r-field-types)))
      {:return-type :bool, :->call-code (constantly false)}

      (let [inner-calls (->> (for [field fields]
                               (MapEntry/create field
                                                (->> (for [l-val-type (-> (get l-field-types field) types/flatten-union-types)
                                                           r-val-type (-> (get r-field-types field) types/flatten-union-types)]
                                                       (MapEntry/create [l-val-type r-val-type]
                                                                        (if (or (= :null l-val-type) (= :null r-val-type))
                                                                          {:return-type :null, :->call-code (constantly nil)}
                                                                          (codegen-call {:f :=, :arg-types [l-val-type r-val-type]}))))
                                                     (into {}))))
                             (into {}))]

        {:return-type [:union #{:bool :null}]
         :continue-call (fn [f [l-code r-code]]
                          (let [res-sym (gensym 'res)]
                            `(let [~res-sym
                                   ~(->> (for [[field inner-calls] inner-calls]
                                           (continue-read (fn [l-val-type l-val-code]
                                                            (continue-read (fn [r-val-type r-val-code]
                                                                             (let [{:keys [return-type continue-call ->call-code]} (get inner-calls [l-val-type r-val-type])]
                                                                               (if continue-call
                                                                                 (continue-call cont-b3-call [l-val-code r-val-code])
                                                                                 (cont-b3-call return-type (->call-code [l-val-code r-val-code])))))
                                                                           (get r-field-types field) r-code (str field)))
                                                          (get l-field-types field) l-code (str field)))
                                         (reduce (fn [l-code r-code]
                                                   `(let [~res-sym ~l-code]
                                                      (if (== -1 ~res-sym)
                                                        -1
                                                        (min ~res-sym ~r-code))))
                                                 1))]
                               (if (zero? ~res-sym)
                                 ~(f :null nil)
                                 ~(f :bool `(== 1 ~res-sym))))))}))))

(defn- codegen-poly-list [[_list [_union inner-types] :as return-type] emitted-els]
  (let [box-sym (gensym 'box)]
    {:batch-bindings [[box-sym `(ValueBox.)]]
     :continue (fn [f]
                 (f return-type
                    (let [type->type-id (->> inner-types
                                             (into {} (map-indexed (fn [idx col-type]
                                                                     [col-type idx]))))]
                      `(reify IPolyVectorReader
                         (~'valueCount [_#] ~(count emitted-els))

                         (~'read [_# idx#]
                          (byte
                           (case idx#
                             ~@(->> emitted-els
                                    (mapcat (fn [{:keys [idx continue]}]
                                              [idx (continue (fn [return-type code]
                                                               (let [type-id (type->type-id return-type)]
                                                                 `(do
                                                                    ~(write-value-code return-type `(.writerForTypeId ~box-sym '~type-id) code)
                                                                    (byte ~type-id)))))]))))))

                         (~'readBoolean [_#] (.readBoolean ~box-sym))
                         (~'readByte [_#] (.readByte ~box-sym))
                         (~'readShort [_#] (.readShort ~box-sym))
                         (~'readInt [_#] (.readInt ~box-sym))
                         (~'readLong [_#] (.readLong ~box-sym))
                         (~'readFloat [_#] (.readFloat ~box-sym))
                         (~'readDouble [_#] (.readDouble ~box-sym))
                         (~'readBytes [_#] (.readBytes ~box-sym))
                         (~'readObject [_#] (.readObject ~box-sym))))))}))

(defmethod codegen-cast [:list :list] [{[_ source-el-type] :source-type
                                        [_ target-el-type :as target-type] :target-type}]
  (assert (types/union? target-el-type))
  (if (types/union? source-el-type)
    (let [target-types ^List (vec (second target-el-type))
          type-id-mapping (->> (second source-el-type)
                               (mapv (fn [source-type]
                                       (.indexOf target-types source-type))))]
      {:return-type target-type
       :->call-code (fn [[code]]
                      `(RemappedTypeIdReader. ~code (byte-array ~type-id-mapping)))})

    (let [type-id (.indexOf ^List (vec (second target-el-type)) source-el-type)]
      {:return-type target-type
       :->call-code (fn [[code]]
                      `(MonoToPolyReader. ~code ~type-id 0))})))

(defmethod codegen-expr :list [{:keys [elements]} opts]
  (let [el-count (count elements)
        emitted-els (->> elements
                         (into [] (map-indexed (fn [idx el]
                                                 (-> (codegen-expr el opts)
                                                     (assoc :idx idx))))))
        el-type (->> (into #{} (map :return-type) emitted-els)
                     (apply types/merge-col-types))
        return-type [:list el-type]]

    (into {:return-type return-type
           :children emitted-els}

          (if (types/union? el-type)
            (codegen-poly-list return-type emitted-els)

            {:continue (fn [f]
                         (let [read-fn (symbol (str "read" (-> el-type types/col-type-head col-type->rw-fn)))]
                           (f return-type
                              `(reify IMonoVectorReader
                                 (~'valueCount [_#] ~el-count)

                                 ~@(when-not (= el-type :null)
                                     [`(~read-fn [_# n#]
                                        (case n#
                                          ~@(->> emitted-els
                                                 (mapcat (fn [{:keys [idx continue]}]
                                                           [idx (continue (fn [_ code] code))])))))])))))}))))

(defmethod codegen-call [:nth :list :int] [{[[_ list-el-type] _n-type] :arg-types}]
  (let [return-type (types/merge-col-types list-el-type :null)]
    {:return-type return-type
     :continue-call (fn [f [list-code n-code]]
                      (let [list-sym (gensym 'len)
                            n-sym (gensym 'n)]
                        `(let [~list-sym ~list-code
                               ~n-sym ~n-code]
                           (if (and (>= ~n-sym 0) (< ~n-sym (.valueCount ~list-sym)))
                             ~(continue-read f list-el-type list-sym n-sym)
                             ~(f :null nil)))))}))

(defmethod codegen-call [:nth :any :int] [_]
  {:return-type :null, :->call-code (constantly nil)})

(defmethod codegen-call [:cardinality :list] [_]
  {:return-type :i32
   :->call-code #(do `(.valueCount ~@%))})

(defn mono-trim-array-view ^xtdb.vector.IMonoVectorReader [^long trimmed-value-count ^IMonoVectorReader lst]
  (reify IMonoVectorReader
    (valueCount [_] trimmed-value-count)
    (readBoolean [_ idx] (.readBoolean lst idx))
    (readByte [_ idx] (.readByte lst idx))
    (readShort [_ idx] (.readShort lst idx))
    (readInt [_ idx] (.readInt lst idx))
    (readLong [_ idx] (.readLong lst idx))
    (readFloat [_ idx] (.readFloat lst idx))
    (readDouble [_ idx] (.readDouble lst idx))
    (readBytes [_ idx] (.readBytes lst idx))
    (readObject [_ idx] (.readObject lst idx))))

(defn poly-trim-array-view ^xtdb.vector.IPolyVectorReader [^long trimmed-value-count ^IPolyVectorReader lst]
  (reify IPolyVectorReader
    (valueCount [_] trimmed-value-count)
    (read [_ idx] (.read lst idx))
    (readBoolean [_] (.readBoolean lst))
    (readByte [_] (.readByte lst))
    (readShort [_] (.readShort lst))
    (readInt [_] (.readInt lst))
    (readLong [_] (.readLong lst))
    (readFloat [_] (.readFloat lst))
    (readDouble [_] (.readDouble lst))
    (readBytes [_] (.readBytes lst))
    (readObject [_] (.readObject lst))))

(defmethod codegen-call [:trim-array :list :int] [{[[_list list-el-type] _n-type] :arg-types}]
  (let [return-type [:list list-el-type]]
    {:return-type return-type
     :continue-call (fn [f [list-code n-code]]
                      (let [list-sym (gensym 'list)
                            n-sym (gensym 'n)]
                        `(let [~list-sym ~list-code
                               ~n-sym (- (.valueCount ~list-sym) ~n-code)]
                           (if (neg? ~n-sym)
                             (throw (err/runtime-err :xtdb.expression/array-element-error
                                                     {::err/message "Data exception - array element error."
                                                      :nlen ~n-sym}))
                             ~(f return-type (if (types/union? list-el-type)
                                               (-> `(poly-trim-array-view ~n-sym ~list-sym)
                                                   (with-tag IPolyVectorReader))
                                               (-> `(mono-trim-array-view ~n-sym ~list-sym)
                                                   (with-tag IMonoVectorReader))))))))}))

(defmethod codegen-call [:= :list :list] [{[[_ l-el-type] [_ r-el-type]] :arg-types}]
  (let [n-sym (gensym 'n)
        len-sym (gensym 'len)
        res-sym (gensym 'res)
        inner-calls (->> (for [l-el-type (types/flatten-union-types l-el-type)
                               r-el-type (types/flatten-union-types r-el-type)]
                           (MapEntry/create [l-el-type r-el-type]
                                            (if (or (= :null l-el-type) (= :null r-el-type))
                                              {:return-type :null, :->call-code (constantly nil)}
                                              (codegen-call {:f :=, :arg-types [l-el-type r-el-type]}))))
                         (into {}))]

    {:return-type [:union #{:bool :null}]
     :continue-call (fn continue-list= [f [l-code r-code]]
                      (let [l-sym (gensym 'l), r-sym (gensym 'r)]
                        ;; this is essentially `(every? = ...)` but with 3VL
                        `(let [~l-sym ~l-code, ~r-sym ~r-code
                               ~(-> res-sym (with-tag 'long))
                               (let [~len-sym (.valueCount ~l-sym)]
                                 (if-not (= ~len-sym (.valueCount ~r-sym))
                                   -1
                                   (loop [~n-sym 0, ~res-sym 1]
                                     (cond
                                       (>= ~n-sym ~len-sym) ~res-sym
                                       (== -1 ~res-sym) -1
                                       :else (recur (inc ~n-sym)
                                                    (min ~res-sym
                                                         ~(continue-read
                                                           (fn cont-l [l-el-type l-el-code]
                                                             (continue-read (fn cont-r [r-el-type r-el-code]
                                                                              (let [{:keys [return-type continue-call ->call-code]} (get inner-calls [l-el-type r-el-type])]
                                                                                (if continue-call
                                                                                  (continue-call cont-b3-call [l-el-code r-el-code])
                                                                                  (cont-b3-call return-type (->call-code [l-el-code r-el-code])))))
                                                                            r-el-type r-sym n-sym))
                                                           l-el-type l-sym n-sym)))))))]
                           (if (zero? ~res-sym)
                             ~(f :null nil)
                             ~(f :bool `(== 1 ~res-sym))))))}))

(def out-vec-sym (gensym 'out_vec))
(def ^:private out-writer-sym (gensym 'out_writer_sym))

(defn batch-bindings [emitted-expr]
  (letfn [(child-seq [{:keys [children] :as expr}]
            (lazy-seq
             (cons expr (mapcat child-seq children))))]
    (->> (for [{:keys [batch-bindings]} (child-seq emitted-expr)
               batch-binding batch-bindings]
           batch-binding)
         (sequence (comp (distinct) cat)))))

(defn write-value-out-code [return-type]
  (zmatch return-type
    [:union inner-types]
    (let [writer-syms (->> inner-types
                           (into {} (map (juxt identity (fn [_] (gensym 'out-writer))))))]
      {:writer-bindings (into [out-writer-sym `(vw/->writer ~out-vec-sym)]
                              (mapcat (fn [[value-type writer-sym]]
                                        [writer-sym `(.legWriter ~out-writer-sym (types/->arrow-type '~value-type))]))
                              writer-syms)

       :write-value-out! (fn [value-type code]
                           (write-value-code value-type (get writer-syms value-type) code))})

    {:writer-bindings [out-writer-sym `(vw/->writer ~out-vec-sym)]
     :write-value-out! (fn [value-type code]
                         (write-value-code value-type out-writer-sym code))}))

(defn- wrap-zone-id-cache-buster [f]
  (fn [expr opts]
    (f expr (assoc opts :zone-id (.getZone *clock*)))))

(def ^:private emit-projection
  "NOTE: we macroexpand inside the memoize on the assumption that
   everything outside yields the same result on the pre-expanded expr - this
   assumption wouldn't hold if macroexpansion created new variable exprs, for example.
   macroexpansion is non-deterministic (gensym), so busts the memo cache."
  (-> (fn [expr opts]
        ;; TODO should lit->param be outside the memoize, s.t. we don't have a cache entry for each literal value?
        (let [expr (prepare-expr expr)
              {:keys [return-type continue] :as emitted-expr} (codegen-expr expr opts)
              {:keys [writer-bindings write-value-out!]} (write-value-out-code return-type)]
          {:!projection-fn (delay
                             (-> `(fn [~(-> rel-sym (with-tag RelationReader))
                                       ~(-> params-sym (with-tag RelationReader))
                                       ~(-> out-vec-sym (with-tag ValueVector))]
                                    (let [~@(batch-bindings emitted-expr)
                                          ~@writer-bindings
                                          row-count# (.rowCount ~rel-sym)]
                                      (dotimes [~idx-sym row-count#]
                                        ~(continue (fn [t c]
                                                     (write-value-out! t c))))))

                                 #_(doto clojure.pprint/pprint)
                                 #_(->> (binding [*print-meta* true]))
                                 eval))

           :return-type return-type}))
      (util/lru-memoize)
      wrap-zone-id-cache-buster))

(defn ->param-types [^RelationReader params]
  (->> params
       (into {} (map (fn [^IVectorReader col]
                       (MapEntry/create
                        (symbol (.getName col))
                        (types/field->col-type (.getField col))))))))

(defn ->param-fields [^RelationReader params]
  (->> params
       (into {} (map (fn [^IVectorReader col]
                       (MapEntry/create
                        (symbol (.getName col))
                        (.getField col)))))))

(defn ->expression-projection-spec ^xtdb.operator.IProjectionSpec [col-name form {:keys [col-types param-types] :as input-types}]
  (let [expr (form->expr form input-types)

        ;; HACK - this runs the analyser (we discard the emission) to get the widest possible out-type.
        widest-out-type (-> (emit-projection expr {:param-types param-types
                                                   :var->col-type col-types})
                            :return-type)]
    (reify IProjectionSpec
      (getColumnName [_] col-name)

      (getColumnType [_] widest-out-type)

      (project [_ allocator in-rel params]
        (let [var->col-type (->> (seq in-rel)
                                 (into {} (map (fn [^IVectorReader iv]
                                                 [(symbol (.getName iv))
                                                  (types/field->col-type (.getField iv))]))))

              {:keys [return-type !projection-fn]} (emit-projection expr {:param-types (->param-types params)
                                                                          :var->col-type var->col-type})
              row-count (.rowCount in-rel)]
          (util/with-close-on-catch [out-vec (-> (types/col-type->field col-name return-type)
                                                 (.createVector allocator))]
            (doto out-vec
              (.setInitialCapacity row-count)
              (.allocateNew))
            (@!projection-fn in-rel params out-vec)
            (.setValueCount out-vec row-count)
            (vr/vec->reader out-vec)))))))

(defn ->expression-relation-selector ^xtdb.operator.IRelationSelector [form input-types]
  (let [projector (->expression-projection-spec "select" (list 'boolean form) input-types)]
    (reify IRelationSelector
      (select [_ al in-rel params]
        (with-open [selection (.project projector al in-rel params)]
          (let [res (IntStream/builder)]
            (dotimes [idx (.valueCount selection)]
              (when (.getBoolean selection idx)
                (.add res idx)))
            (.toArray (.build res))))))))
