(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.error :as err])
  (:import (clojure.lang Keyword MapEntry)
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.types LegType)
           (core2.vector IIndirectRelation IIndirectVector IRowCopier IVectorWriter)
           (core2.vector.extensions KeywordType UuidType)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate ZonedDateTime ZoneId ZoneOffset Period)
           (java.time.temporal ChronoField ChronoUnit)
           (java.util Date HashMap LinkedList)
           (java.util.function IntUnaryOperator)
           (java.util.stream IntStream)
           (org.apache.arrow.vector BigIntVector BitVector DurationVector FieldVector IntVector ValueVector PeriodDuration)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector StructVector)
           (org.apache.arrow.vector.types DateUnit TimeUnit Types Types$MinorType IntervalUnit)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Duration ArrowType$ExtensionType ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Null ArrowType$Timestamp ArrowType$Utf8 Field FieldType ArrowType$Time ArrowType$Interval)
           (java.util.regex Pattern)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti parse-list-form
  (fn [[f & args] env]
    f)
  :default ::default)

(defn form->expr [form {:keys [col-names params locals] :as env}]
  (cond
    (symbol? form) (cond
                     (contains? locals form) {:op :local, :local form}
                     (contains? params form) (let [param-v (get params form)]
                                               {:op :param, :param form,
                                                :param-type (.arrowType (types/value->leg-type param-v))
                                                :param-class (class param-v)})
                     (contains? col-names form) {:op :variable, :variable form}
                     :else (throw (err/illegal-arg :unknown-symbol
                                                   {::err/message (format "Unknown symbol: '%s'" form)
                                                    :symbol form})))

    (map? form) (do
                  (when-not (every? keyword? (keys form))
                    (throw (IllegalArgumentException. (str "keys to struct must be keywords: " (pr-str form)))))
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
    (throw (IllegalArgumentException. (str "'if' expects 3 args: " (pr-str form)))))

  (let [[pred then else] args]
    {:op :if,
     :pred (form->expr pred env),
     :then (form->expr then env),
     :else (form->expr else env)}))

(defmethod parse-list-form 'let [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'let' expects 2 args - bindings + body"
                                           (pr-str form)))))

  (let [[bindings body] args]
    (when-not (or (nil? bindings) (sequential? bindings))
      (throw (IllegalArgumentException. (str "'let' expects a sequence of bindings: "
                                             (pr-str form)))))

    (if-let [[local expr-form & more-bindings] (seq bindings)]
      (do
        (when-not (symbol? local)
          (throw (IllegalArgumentException. (str "bindings in `let` should be symbols: "
                                                 (pr-str local)))))
        {:op :let
         :local local
         :expr (form->expr expr-form env)
         :body (form->expr (list 'let more-bindings body)
                           (update env :locals (fnil conj #{}) local))})

      (form->expr body env))))

(defn- ->dot-expr [struct-expr field-form env]
  (if (symbol? field-form)
    {:op :dot-const-field
     :struct-expr struct-expr
     :field field-form}
    {:op :dot
     :struct-expr struct-expr
     :field-expr (form->expr field-form env)}))

(defmethod parse-list-form '. [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'.' expects 2 args: " (pr-str form)))))

  (let [[struct field] args]
    (->dot-expr (form->expr struct env) field env)))

(defmethod parse-list-form '.. [[_ & args :as form] env]
  (let [[struct & fields] args]
    (when-not (seq fields)
      (throw (IllegalArgumentException. (str "'..' expects at least 2 args: " (pr-str form)))))
    (when-not (every? symbol? fields)
      (throw (IllegalArgumentException. (str "'..' expects symbol fields: " (pr-str form)))))
    (reduce (fn [struct-expr field-form]
              (->dot-expr struct-expr field-form env))
            (form->expr struct env)
            (rest args))))

(defmethod parse-list-form 'nth [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'nth' expects 2 args: " (pr-str form)))))

  (let [[coll-form n-form] args]
    (if (integer? n-form)
      {:op :nth-const-n
       :coll-expr (form->expr coll-form env)
       :n n-form}
      {:op :nth
       :coll-expr (form->expr coll-form env)
       :n-expr (form->expr n-form env)})))

(defmethod parse-list-form ::default [[f & args] env]
  {:op :call, :f f, :args (mapv #(form->expr % env) args)})

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(def type->cast
  {ArrowType$Bool/INSTANCE 'boolean
   (.getType Types$MinorType/TINYINT) 'byte
   (.getType Types$MinorType/SMALLINT) 'short
   (.getType Types$MinorType/INT) 'int
   types/bigint-type 'long
   types/float8-type 'double
   types/timestamp-micro-tz-type 'long
   types/duration-micro-type 'long})

(def idx-sym (gensym 'idx))
(def rel-sym (gensym 'rel))
(def params-sym (gensym 'params))

(defmulti with-batch-bindings :op, :default ::default)
(defmethod with-batch-bindings ::default [expr] expr)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-expr
  "Returns a map containing
    * `:return-types` (set)
    * `:continue` (fn).
      Returned fn expects a function taking a single return-type and the emitted code for that return-type.
      May be called multiple times if there are multiple return types.
      Returned fn returns the emitted code for the expression (including the code supplied by the callback)."
  (fn [{:keys [op]} {:keys [var->types]}]
    op))

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-value
  (fn [val-class code] val-class)
  :default ::default)

(defmethod emit-value ::default [_ code] code)
(defmethod emit-value Date [_ code] `(Math/multiplyExact (.getTime ~code) 1000))
(defmethod emit-value Instant [_ code] `(util/instant->micros ~code))
(defmethod emit-value Duration [_ code] `(quot (.toNanos ~code) 1000))
(defmethod emit-value (Class/forName "[B") [_ code] `(ByteBuffer/wrap ~code))

(defmethod emit-value String [_ code]
  `(-> (.getBytes ~code StandardCharsets/UTF_8) (ByteBuffer/wrap)))

(defmethod emit-value Keyword [_ code] (emit-value String `(str (symbol ~code))))

(defmethod emit-value LocalDate [_ code]
  ;; if literal, we can just yield a long directly
  (if (instance? LocalDate code)
    (.toEpochDay ^LocalDate code)
    `(.toEpochDay ~code)))

(defmethod emit-value PeriodDuration [_ code]
  `(PeriodDuration. (Period/parse ~(str (.getPeriod ^PeriodDuration code)))
                    (Duration/ofNanos ~(.toNanos (.getDuration ^PeriodDuration code)))))

(defmethod codegen-expr :literal [{:keys [literal]} _]
  (let [return-type (.arrowType (types/value->leg-type literal))
        literal-type (class literal)]
    {:return-types #{return-type}
     :continue (fn [f]
                 (f return-type (emit-value literal-type literal)))
     :literal literal}))

(defn lit->param [{:keys [op] :as expr}]
  (if (= op :literal)
    (let [{:keys [literal]} expr]
      {:op :param, :param (gensym 'lit),
       :literal literal
       :param-class (class literal)
       :param-type (.arrowType (types/value->leg-type literal))})
    expr))

(defmethod with-batch-bindings :param [{:keys [param param-class] :as expr}]
  (-> expr
      (assoc :batch-bindings
             [[param
               (emit-value param-class
                           (:literal expr
                                     (cond-> `(get ~params-sym '~param)
                                       param-class (with-tag param-class))))]])))

(defmethod codegen-expr :param [{:keys [param param-type] :as expr} _]
  (into {:return-types #{param-type}
         :continue (fn [f]
                     (f param-type param))}
        (select-keys expr #{:literal})))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti get-value-form
  (fn [arrow-type vec-sym idx-sym]
    (class arrow-type))
  :default ::default)

(defmethod get-value-form ArrowType$Null [_ _ _] nil)
(defmethod get-value-form ArrowType$Bool [_ vec-sym idx-sym] `(= 1 (.get ~vec-sym ~idx-sym)))
(defmethod get-value-form ArrowType$FloatingPoint [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Int [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Timestamp [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Duration [_ vec-sym idx-sym] `(DurationVector/get (.getDataBuffer ~vec-sym) ~idx-sym))
(defmethod get-value-form ArrowType$Utf8 [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Binary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$FixedSizeBinary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))

;; will unify both possible vector representations
;; as an epoch int, do not think it is worth branching for both cases in all date functions.
(defmethod get-value-form ArrowType$Date [^ArrowType$Date type vec-sym idx-sym]
  (util/case-enum (.getUnit type)
    DateUnit/DAY `(.get ~vec-sym ~idx-sym)
    DateUnit/MILLISECOND `(int (quot (.get ~vec-sym ~idx-sym) 86400000))))

;; unifies to nanos (of day) long
(defmethod get-value-form ArrowType$Time [^ArrowType$Time type vec-sym idx-sym]
  (util/case-enum (.getUnit type)
    TimeUnit/NANOSECOND `(.get ~vec-sym ~idx-sym)
    TimeUnit/MICROSECOND `(* 1e3 (.get ~vec-sym ~idx-sym))
    TimeUnit/MILLISECOND `(* 1e6 (long (get ~vec-sym ~idx-sym)))
    TimeUnit/SECOND `(* 1e9 (long (get ~vec-sym ~idx-sym)))))

;; we will box for simplicity given bigger than a long in worst case, may later change to a more primitive
(defmethod get-value-form ArrowType$Interval [_type vec-sym idx-sym]
  `(types/get-object ~vec-sym ~idx-sym))

(defmethod get-value-form ArrowType$ExtensionType
  [^ArrowType$ExtensionType arrow-type vec-sym idx-sym]
  ;; a reasonable default, but implement it for more specific types if this doesn't work
  (get-value-form (.storageType arrow-type) `(.getUnderlyingVector ~vec-sym) idx-sym))

(defmethod with-batch-bindings :variable [{:keys [variable] :as expr}]
  (-> expr
      (assoc :batch-bindings
             [[(-> variable (with-tag IIndirectVector))
               `(.vectorForName ~rel-sym ~(name variable))]])))

(defmethod codegen-expr :variable [{:keys [variable idx], :or {idx idx-sym}} {:keys [var->types]}]
  (let [field-types (or (get var->types variable)
                        (throw (AssertionError. (str "unknown variable: " variable))))
        var-idx-sym (gensym 'var-idx)
        var-vec-sym (gensym 'var-vec)]
    (if-not (vector? field-types)
      (let [^FieldType field-type field-types
            arrow-type (.getType field-type)
            vec-type (types/arrow-type->vector-type arrow-type)
            nullable? (.isNullable field-type)]

        {:return-types (cond-> #{arrow-type}
                         nullable? (conj types/null-type))
         :continue (fn [f]
                     `(let [~var-idx-sym (.getIndex ~variable ~idx)
                            ~(-> var-vec-sym (with-tag vec-type)) (.getVector ~variable)]
                        ~(let [get-value (f arrow-type (get-value-form arrow-type var-vec-sym var-idx-sym))]
                           (if nullable?
                             `(if (.isNull ~var-vec-sym ~var-idx-sym)
                                ~(f types/null-type nil)
                                ~get-value)
                             get-value))))})

      {:return-types
       (->> field-types
            (into #{} (mapcat (fn [^FieldType field-type]
                                (cond-> #{(.getType field-type)}
                                  (.isNullable field-type) (conj ArrowType$Null/INSTANCE))))))

       :continue
       (fn [f]
         `(let [~var-idx-sym (.getIndex ~variable ~idx)
                ~(-> var-vec-sym (with-tag DenseUnionVector)) (.getVector ~variable)]
            (case (.getTypeId ~var-vec-sym ~var-idx-sym)
              ~@(->> field-types
                     (map-indexed
                      (fn [type-id ^FieldType field-type]
                        (let [arrow-type (.getType field-type)]
                          [type-id
                           (f arrow-type (get-value-form arrow-type
                                                         (-> `(.getVectorByType ~var-vec-sym ~type-id)
                                                             (with-tag (types/arrow-type->vector-type arrow-type)))
                                                         `(.getOffset ~var-vec-sym ~var-idx-sym)))])))
                     (apply concat)))))})))

(defn- wrap-mono-return [{:keys [return-types continue]}]
  {:return-types return-types
   :continue (if (= 1 (count return-types))
               ;; only generate the surrounding code once if we're monomorphic
               (fn [f]
                 (f (first return-types)
                    (continue (fn [_ code] code))))

               continue)})

(defmethod codegen-expr :if [{:keys [pred then else]} opts]
  (let [{p-rets :return-types, p-cont :continue} (codegen-expr pred opts)
        {t-rets :return-types, t-cont :continue} (codegen-expr then opts)
        {e-rets :return-types, e-cont :continue} (codegen-expr else opts)
        return-types (set/union t-rets e-rets)]
    (when-not (= #{ArrowType$Bool/INSTANCE} p-rets)
      (throw (IllegalArgumentException. (str "pred expression doesn't return boolean "
                                             (pr-str p-rets)))))

    (-> {:return-types return-types
         :continue (fn [f]
                     `(if ~(p-cont (fn [_ code] code))
                        ~(t-cont f)
                        ~(e-cont f)))}
        (wrap-mono-return))))

(defmethod codegen-expr :local [{:keys [local]} {:keys [local-types]}]
  (let [return-type (get local-types local)]
    {:return-types #{return-type}
     :continue (fn [f] (f return-type local))}))

(defmethod codegen-expr :let [{:keys [local expr body]} opts]
  (let [{continue-expr :continue, :as emitted-expr} (codegen-expr expr opts)
        emitted-bodies (->> (for [local-type (:return-types emitted-expr)]
                              (MapEntry/create local-type
                                               (codegen-expr body (assoc-in opts [:local-types local] local-type))))
                            (into {}))
        return-types (into #{} (mapcat :return-types) (vals emitted-bodies))]
    (-> {:return-types return-types
         :continue (fn [f]
                     (continue-expr (fn [local-type code]
                                      `(let [~local ~code]
                                         ~((:continue (get emitted-bodies local-type)) f)))))}
        (wrap-mono-return))))

(defmethod codegen-expr :if-some [{:keys [local expr then else]} opts]
  (let [{continue-expr :continue, expr-rets :return-types} (codegen-expr expr opts)
        emitted-thens (->> (for [local-type (-> expr-rets
                                                (disj types/null-type))]
                             (MapEntry/create local-type
                                              (codegen-expr then (assoc-in opts [:local-types local] local-type))))
                           (into {}))
        then-rets (into #{} (mapcat :return-types) (vals emitted-thens))]

    (-> (if-not (contains? expr-rets types/null-type)
          {:return-types then-rets
           :continue (fn [f]
                       (continue-expr (fn [local-type code]
                                        `(let [~local ~code]
                                           ~((:continue (get emitted-thens local-type)) f)))))}

          (let [{e-rets :return-types, e-cont :continue} (codegen-expr else opts)
                return-types (into then-rets e-rets)]
            {:return-types return-types
             :continue (fn [f]
                         (continue-expr (fn [local-type code]
                                          (if (= local-type types/null-type)
                                            (e-cont f)
                                            `(let [~local ~code]
                                               ~((:continue (get emitted-thens local-type)) f))))))}))
        (wrap-mono-return))))

(defmulti codegen-call
  "Expects a map containing both the expression and an `:arg-types` key - a vector of ArrowTypes.
   This `:arg-types` vector should be monomorphic - if the args are polymorphic, call this multimethod multiple times.

   Returns a map containing
    * `:return-types` (set)
    * `:continue-call` (fn).
      Returned fn expects:
      * a function taking a single return-type and the emitted code for that return-type.
      * the code for the arguments to the call
      May be called multiple times if there are multiple return types.
      Returned fn returns the emitted code for the expression (including the code supplied by the callback)."
  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (name f)) (map class arg-types))))
  :hierarchy #'types/arrow-type-hierarchy)

(defmethod codegen-expr :call [{:keys [args] :as expr} opts]
  (let [emitted-args (mapv #(codegen-expr % opts) args)
        all-arg-types (reduce (fn [acc {:keys [return-types]}]
                                (for [el acc
                                      return-type return-types]
                                  (conj el return-type)))
                              [[]]
                              emitted-args)

        emitted-calls (->> (for [arg-types all-arg-types]
                             (MapEntry/create arg-types
                                              (codegen-call (assoc expr
                                                                   :args emitted-args
                                                                   :arg-types arg-types))))
                           (into {}))

        return-types (->> emitted-calls
                          (into #{} (mapcat (comp :return-types val))))]

    (-> {:return-types return-types
         :continue (fn continue-call-expr [handle-emitted-expr]
                     (let [build-args-then-call
                           (reduce (fn step [build-next-arg {continue-this-arg :continue}]
                                     ;; step: emit this arg, and pass it through to the inner build-fn
                                     (fn continue-building-args [arg-types emitted-args]
                                       (continue-this-arg (fn [arg-type emitted-arg]
                                                            (build-next-arg (conj arg-types arg-type)
                                                                            (conj emitted-args emitted-arg))))))

                                   ;; innermost fn - we're done, call continue-call for these types
                                   (fn call-with-built-args [arg-types emitted-args]
                                     (let [{:keys [continue-call]} (get emitted-calls arg-types)]
                                       (continue-call handle-emitted-expr emitted-args)))

                                   ;; reverse because we're working inside-out
                                   (reverse emitted-args))]

                       (build-args-then-call [] [])))}
        (wrap-mono-return))))

(defn mono-fn-call [return-type wrap-args-f]
  {:continue-call (fn [f emitted-args]
                    (f return-type (-> emitted-args wrap-args-f)))
   :return-types #{return-type}})

(def call-returns-null
  (mono-fn-call ArrowType$Null/INSTANCE (constantly nil)))

(doseq [[f-kw cmp] [[:< #(do `(neg? ~%))]
                    [:<= #(do `(not (pos? ~%)))]
                    [:> #(do `(pos? ~%))]
                    [:>= #(do `(not (neg? ~%)))]]
        :let [f-sym (symbol (name f-kw))]]

  (doseq [arrow-type #{::types/Number ArrowType$Timestamp ArrowType$Duration ArrowType$Date}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [_]
      (mono-fn-call types/bool-type #(do `(~f-sym ~@%)))))

  (doseq [arrow-type #{ArrowType$Binary ArrowType$Utf8}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [_]
      (mono-fn-call types/bool-type #(cmp `(util/compare-nio-buffers-unsigned ~@%)))))

  (defmethod codegen-call [f-kw ::types/Object ::types/Object] [_]
    (mono-fn-call types/bool-type #(cmp `(compare ~@%)))))

(defmethod codegen-call [:= ::types/Number ::types/Number] [_]
  (mono-fn-call types/bool-type #(do `(== ~@%))))

(defmethod codegen-call [:= ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(= ~@%))))

(defmethod codegen-call [:<> ::types/Number ::types/Number] [_]
  (mono-fn-call types/bool-type #(do `(not (== ~@%)))))

(defmethod codegen-call [:<> ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(not= ~@%))))

(doseq [f-kw #{:= :< :<= :> :>= :<>}]
  (defmethod codegen-call [f-kw ::types/Object ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f-kw ArrowType$Null ::types/Object] [_] call-returns-null)
  (defmethod codegen-call [f-kw ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defmethod codegen-call [:and ArrowType$Bool ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(and ~@%))))

(defmethod codegen-call [:and ArrowType$Bool ArrowType$Null] [_]
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [x-code _]]
                    `(if-not ~x-code
                       ~(f types/bool-type false)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:and ArrowType$Null ArrowType$Bool] [_]
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [_ y-code]]
                    `(if-not ~y-code
                       ~(f types/bool-type false)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:and ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:or ArrowType$Bool ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(or ~@%))))

(defmethod codegen-call [:or ArrowType$Bool ArrowType$Null] [_]
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [x-code _]]
                    `(if ~x-code
                       ~(f types/bool-type true)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:or ArrowType$Null ArrowType$Bool] [_]
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [_ y-code]]
                    `(if ~y-code
                       ~(f types/bool-type true)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:or ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:not ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(not ~@%))))

(defmethod codegen-call [:not ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:true? ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(true? ~@%))))

(defmethod codegen-call [:false? ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(false? ~@%))))

(defmethod codegen-call [:nil? ArrowType$Null] [_]
  (mono-fn-call types/bool-type (constantly true)))

(doseq [f #{:true? :false? :nil?}]
  (defmethod codegen-call [f ::types/Object] [_]
    (mono-fn-call types/bool-type (constantly false))))

(defmethod codegen-call [:boolean ArrowType$Null] [_]
  (mono-fn-call types/bool-type (constantly false)))

(defmethod codegen-call [:boolean ArrowType$Bool] [_]
  (mono-fn-call types/bool-type first))

(defmethod codegen-call [:boolean ::types/Object] [_]
  (mono-fn-call types/bool-type (constantly true)))

(defn- with-math-integer-cast
  "java.lang.Math's functions only take int or long, so we introduce an up-cast if need be"
  [^ArrowType$Int arrow-type emitted-args]
  (let [arg-cast (if (= 64 (.getBitWidth arrow-type)) 'long 'int)]
    (map #(list arg-cast %) emitted-args)))

(defmethod codegen-call [:+ ArrowType$Int ArrowType$Int] [{:keys [arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:return-types #{return-type}
     :continue-call (fn [f emitted-args]
                      (f return-type
                         (list (type->cast return-type)
                               `(Math/addExact ~@(with-math-integer-cast return-type emitted-args)))))}))

(defmethod codegen-call [:+ ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(+ ~@%))))

(defmethod codegen-call [:- ArrowType$Int ArrowType$Int] [{:keys [arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:return-types #{return-type}
     :continue-call (fn [f emitted-args]
                      (f return-type
                         (list (type->cast return-type)
                               `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args)))))}))

(defmethod codegen-call [:- ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(- ~@%))))

(defmethod codegen-call [:- ArrowType$Int] [{[^ArrowType$Int x-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       (list (type->cast x-type)
                             `(Math/negateExact ~@(with-math-integer-cast x-type emitted-args)))))
   :return-types #{x-type}})

(defmethod codegen-call [:- ::types/Number] [{[x-type] :arg-types}] (mono-fn-call x-type #(do `(- ~@%))))
(defmethod codegen-call [:- ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:* ArrowType$Int ArrowType$Int] [{:keys [arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)
        arg-cast (if (= 64 (.getBitWidth return-type)) 'long 'int)]
    {:return-types #{return-type}
     :continue-call (fn [f emitted-args]
                      (f return-type
                         (list (type->cast return-type)
                               `(Math/multiplyExact ~@(map #(list arg-cast %) emitted-args)))))}))

(defmethod codegen-call [:* ::types/Number ::types/Number] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-types #{return-type}
     :continue-call (fn [f emitted-args]
                      (f return-type `(* ~@emitted-args)))}))

(defmethod codegen-call [:mod ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(mod ~@%))))

(defmethod codegen-call [:/ ArrowType$Int ArrowType$Int] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(quot ~@%))))

(defmethod codegen-call [:/ ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(/ ~@%))))

(defmethod codegen-call [:max ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(Math/max ~@%))))

(defmethod codegen-call [:min ::types/Number ::types/Number] [{:keys [arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(Math/min ~@%))))

(defmethod codegen-call [:power ::types/Number ::types/Number] [_]
  (mono-fn-call types/float8-type #(do `(Math/pow ~@%))))

(defmethod codegen-call [:log ::types/Number ::types/Number] [_]
  (mono-fn-call types/float8-type (fn [[base x]]
                                    `(/ (Math/log ~x) (Math/log ~base)))))

(doseq [f #{:+ :- :* :/ :mod :min :max :power :log}]
  (defmethod codegen-call [f ::types/Number ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ::types/Number] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defmethod codegen-call [:double ::types/Number] [_]
  (mono-fn-call types/float8-type #(do `(double ~@%))))

(defmethod codegen-call [:double ArrowType$Null] [_] call-returns-null)

;; TODO extend min/max to non-numeric - numeric handled by `java.lang.Math`, below
(doseq [f #{:min :max}]
  (defmethod codegen-call [f ::types/Number ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ::types/Number] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defn like->regex [like-pattern]
  (-> like-pattern
      (Pattern/quote)
      (.replace "%" "\\E.*\\Q")
      (.replace "_" "\\E.\\Q")
      (->> (format "^%s$"))
      re-pattern))

(defmethod codegen-call [:like ArrowType$Utf8 ArrowType$Utf8] [{[_ {:keys [literal]}] :args}]
  {:return-types #{types/bool-type}
   :continue-call (fn [f [haystack-code needle-code]]
                    (f types/bool-type
                       `(boolean (re-find ~(if literal
                                             (like->regex literal)
                                             `(like->regex (resolve-string ~needle-code)))
                                          (resolve-string ~haystack-code)))))})

(defmethod codegen-call [:like ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:like ArrowType$Binary ArrowType$Binary] [{[_ {:keys [literal]}] :args}]
  {:return-types #{types/bool-type}
   :continue-call (fn [f [haystack-code needle-code]]
                    (f types/bool-type
                       `(boolean (re-find ~(if literal
                                             (like->regex literal)
                                             `(like->regex (resolve-string ~needle-code)))
                                          (resolve-string ~haystack-code)))))})

(defmethod codegen-call [:like ArrowType$Binary ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Binary] [_] call-returns-null)

(defmethod codegen-call [:substring ::types/Object ArrowType$Int ArrowType$Int] [_]
  {:return-types #{ArrowType$Utf8/INSTANCE}
   :continue-call (fn [f [x start length]]
                    (f ArrowType$Utf8/INSTANCE
                       `(ByteBuffer/wrap (.getBytes (subs (resolve-string ~x) (dec ~start) (+ (dec ~start) ~length))
                                                    StandardCharsets/UTF_8))))})

(defmethod codegen-call [:character-length ArrowType$Utf8] [_]
  (mono-fn-call types/int-type #(do `(.length (.decode StandardCharsets/UTF_8 ~@%)))))

(defmethod codegen-call [:octet-length ArrowType$Utf8] [_]
  (mono-fn-call types/int-type #(do `(.limit ~(-> (first %) (with-tag ByteBuffer))))))

(defmethod codegen-call [:octet-length ArrowType$Binary] [_]
  (mono-fn-call types/int-type #(do `(.limit ~(-> (first %) (with-tag ByteBuffer))))))

(defmethod codegen-call [:extract ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} _] :args}]
  {:return-types #{types/bigint-type}
   :continue-call (fn [f [_ ts-code]]
                    (f types/bigint-type
                       `(.get (.atOffset ^Instant (util/micros->instant ~ts-code) ZoneOffset/UTC)
                              ~(case field
                                 "YEAR" `ChronoField/YEAR
                                 "MONTH" `ChronoField/MONTH_OF_YEAR
                                 "DAY" `ChronoField/DAY_OF_MONTH
                                 "HOUR" `ChronoField/HOUR_OF_DAY
                                 "MINUTE" `ChronoField/MINUTE_OF_HOUR))))})

(defmethod codegen-call [:extract ArrowType$Utf8 ArrowType$Date] [{[{field :literal} _] :args}]
  {:return-types  #{types/int-type}
   :continue-call (fn [f [_ epoch-day-code]]
                    (f types/int-type
                       (case field
                         ;; we could inline the math here, but looking at sources, there is some nuance.
                         "YEAR" `(.getYear (LocalDate/ofEpochDay ~epoch-day-code))
                         "MONTH" `(.getMonthValue (LocalDate/ofEpochDay ~epoch-day-code))
                         "DAY" `(.getDayOfMonth (LocalDate/ofEpochDay ~epoch-day-code))
                         "HOUR" 0
                         "MINUTE" 0)))})

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} _] :args, [_ date-type] :arg-types}]
  {:return-types #{date-type}
   :continue-call (fn [f [_ x]]
                    (f date-type
                       `(util/instant->micros (-> (util/micros->instant ~x)
                                                  ;; TODO only need to create the ZoneId once per batch
                                                  ;; but getting calls to have batch-bindings isn't straightforward
                                                  (ZonedDateTime/ofInstant (ZoneId/of ~(.getTimezone ^ArrowType$Timestamp date-type)))
                                                  ~(case field
                                                     "YEAR" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfYear 1))
                                                     "MONTH" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfMonth 1))
                                                     `(.truncatedTo ~(case field
                                                                       "DAY" `ChronoUnit/DAYS
                                                                       "HOUR" `ChronoUnit/HOURS
                                                                       "MINUTE" `ChronoUnit/MINUTES
                                                                       "SECOND" `ChronoUnit/SECONDS
                                                                       "MILLISECOND" `ChronoUnit/MILLIS
                                                                       "MICROSECOND" `ChronoUnit/MICROS)))
                                                  (.toInstant)))))})

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Date] [{[{field :literal} _] :args}]
  {:return-types #{types/date-day-type}
   :continue-call (fn [f [_ epoch-day-code]]
                    (f types/date-day-type
                       (case field
                         "YEAR" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfYear 1) .toEpochDay)
                         "MONTH" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfMonth 1) .toEpochDay)
                         "DAY" epoch-day-code
                         "HOUR" epoch-day-code
                         "MINUTE" epoch-day-code)))})

(def ^:dynamic ^java.time.Clock *clock* (Clock/systemDefaultZone))

(defn- bound-precision ^long [^long precision]
  (-> precision (max 0) (min 9)))

(def ^:private precision-timeunits
  [TimeUnit/SECOND
   TimeUnit/MILLISECOND TimeUnit/MILLISECOND TimeUnit/MILLISECOND
   TimeUnit/MICROSECOND TimeUnit/MICROSECOND TimeUnit/MICROSECOND
   TimeUnit/NANOSECOND TimeUnit/NANOSECOND TimeUnit/NANOSECOND])

(def ^:private seconds-multiplier (mapv long [1e0 1e3 1e3 1e3 1e6 1e6 1e6 1e9 1e9 1e9]))
(def ^:private nanos-divisor (mapv long [1e9 1e6 1e6 1e6 1e3 1e3 1e3 1e0 1e0 1e0]))
(def ^:private precision-modulus (mapv long [1e0 1e2 1e1 1e0 1e2 1e1 1e0 1e2 1e1 1e0]))

(defn- truncate-for-precision [code precision]
  (let [^long modulus (precision-modulus precision)]
    (if (= modulus 1)
      code
      `(* ~modulus (quot ~code ~modulus)))))

(defn- current-timestamp [^long precision]
  (let [precision (bound-precision precision)
        zone-id (.getZone *clock*)
        ret-type (ArrowType$Timestamp. (precision-timeunits precision) (str zone-id))]
    {:return-types #{ret-type}
     :continue-call (fn [f _]
                      (f ret-type
                         (-> `(long (let [inst# (.instant *clock*)]
                                      (+ (* ~(seconds-multiplier precision) (.getEpochSecond inst#))
                                         (quot (.getNano inst#) ~(nanos-divisor precision)))))
                             (truncate-for-precision precision))))}))

(def ^:private default-time-precision 6)

(defmethod codegen-call [:current-timestamp] [_]
  (current-timestamp default-time-precision))

(defmethod codegen-call [:current-timestamp ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-timestamp precision))

(defmethod codegen-call [:current-date] [_]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Dates to be in UTC.
  ;; we then turn DateDays into LocalDates, which confuses things further.
  {:return-types #{types/date-day-type}
   :continue-call (fn [f _]
                    (f types/date-day-type
                       `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                                  (.toLocalDate)
                                  (.toEpochDay)))))})

(def timeunit->bitwidth
  {TimeUnit/SECOND 32
   TimeUnit/MILLISECOND 32
   TimeUnit/MICROSECOND 64
   TimeUnit/NANOSECOND 64})

(defn- current-time [^long precision]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Times to be in UTC.
  ;; we then turn times into LocalTimes, which confuses things further.
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)
        ret-type (ArrowType$Time. time-unit (timeunit->bitwidth time-unit))]
    {:return-types #{ret-type}
     :continue-call (fn [f _]
                      (f ret-type
                         (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                                        (.toLocalTime)
                                        (.toNanoOfDay)
                                        (quot ~(nanos-divisor precision))))
                             (truncate-for-precision precision))))}))

(defmethod codegen-call [:current-time] [_]
  (current-time default-time-precision))

(defmethod codegen-call [:current-time ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-time precision))

(defn- local-timestamp [^long precision]
  (let [precision (bound-precision precision)
        ret-type (ArrowType$Timestamp. (precision-timeunits precision) nil)]
    {:return-types #{ret-type}
     :continue-call (fn [f _]
                      (f ret-type
                         (-> `(long (let [ldt# (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                                                   (.toLocalDateTime))]
                                      (+ (* (.toEpochSecond ldt# ZoneOffset/UTC) ~(seconds-multiplier precision))
                                         (quot (.getNano ldt#) ~(nanos-divisor precision)))))
                             (truncate-for-precision precision))))}))

(defmethod codegen-call [:local-timestamp] [_]
  (local-timestamp default-time-precision))

(defmethod codegen-call [:local-timestamp ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-timestamp precision))

(defn- local-time [^long precision]
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)
        ret-type (ArrowType$Time. time-unit (timeunit->bitwidth time-unit))]
    {:return-types #{ret-type}
     :continue-call (fn [f _]
                      (f ret-type
                         (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                                        (.toLocalTime)
                                        (.toNanoOfDay)
                                        (quot ~(nanos-divisor precision))))
                             (truncate-for-precision precision))))}))

(defmethod codegen-call [:local-time] [_]
  (local-time default-time-precision))

(defmethod codegen-call [:local-time ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-time precision))

(defmethod codegen-call [:abs ::types/Number] [{[numeric-type] :arg-types}]
  {:return-types #{numeric-type}
   :continue-call (fn [f emitted-args]
                    (f numeric-type
                       `(Math/abs ~@emitted-args)))})

(defmethod codegen-call [:abs ArrowType$Null] [_] call-returns-null)

(doseq [[math-op math-method] {:sin 'Math/sin
                               :cos 'Math/cos
                               :tan 'Math/tan
                               :sinh 'Math/sinh
                               :cosh 'Math/cosh
                               :tanh 'Math/tanh
                               :asin 'Math/asin
                               :acos 'Math/acos
                               :atan 'Math/atan
                               :sqrt 'Math/sqrt
                               :ln 'Math/log
                               :log10 'Math/log10
                               :exp 'Math/exp
                               :floor 'Math/floor
                               :ceil 'Math/ceil}]
  (defmethod codegen-call [math-op ::types/Number] [{[numeric-type] :arg-types}]
    {:return-types #{types/float8-type}
     :continue-call (fn [f emitted-args]
                      (f numeric-type
                         `(~math-method ~@emitted-args)))})

  (defmethod codegen-call [math-op ArrowType$Null] [_] call-returns-null))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti set-value-form
  (fn [arrow-type out-vec-sym idx-sym code]
    (class arrow-type)))

(defmethod set-value-form ArrowType$Null [_ _out-vec-sym _idx-sym _code])

(defmethod set-value-form ArrowType$Bool [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (if ~code 1 0)))

(defmethod set-value-form ArrowType$Int [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$FloatingPoint [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (double ~code)))

(defn- set-bytebuf-form [out-vec-sym idx-sym code]
  `(let [^ByteBuffer buf# ~code
         pos# (.position buf#)]
     (.setSafe ~out-vec-sym ~idx-sym buf#
               pos# (.remaining buf#))
     ;; setSafe mutates the buffer's position, so we reset it
     (.position buf# pos#)))

(defmethod set-value-form ArrowType$Utf8 [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form ArrowType$Binary [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form ArrowType$Timestamp [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Duration [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Date [_ out-vec-sym idx-sym code]
  ;; assuming we are writing to a date day vector, as that is the canonical representation
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Time [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Interval [_ out-vec-sym idx-sym code]
  `(let [period-duration# ~code
         period# (.getPeriod period-duration#)
         duration# (.getDuration period-duration#)]
     (.set ~out-vec-sym ~idx-sym (.toTotalMonths period#) (.getDays period#) (.toNanos duration#))))

(def ^:private out-vec-sym (gensym 'out-vec))
(def ^:private out-writer-sym (gensym 'out-writer-sym))

(defmulti extension-type-literal-form class)

(defmethod extension-type-literal-form KeywordType [_] `KeywordType/INSTANCE)
(defmethod extension-type-literal-form UuidType [_] `UuidType/INSTANCE)

(defn- arrow-type-literal-form [^ArrowType arrow-type]
  (letfn [(timestamp-type-literal [time-unit-literal]
            `(ArrowType$Timestamp. ~time-unit-literal ~(.getTimezone ^ArrowType$Timestamp arrow-type)))]
    (let [minor-type-name (.name (Types/getMinorTypeForArrowType arrow-type))]
      (case minor-type-name
        "DURATION" `(ArrowType$Duration. ~(symbol (name `TimeUnit) (.name (.getUnit ^ArrowType$Duration arrow-type))))

        "TIMESTAMPSECTZ" (timestamp-type-literal `TimeUnit/SECOND)
        "TIMESTAMPMILLITZ" (timestamp-type-literal `TimeUnit/MILLISECOND)
        "TIMESTAMPMICROTZ" (timestamp-type-literal `TimeUnit/MICROSECOND)
        "TIMESTAMPNANOTZ" (timestamp-type-literal `TimeUnit/NANOSECOND)

        "EXTENSIONTYPE" `~(extension-type-literal-form arrow-type)

        "DATEDAY" `(ArrowType$Date. DateUnit/DAY)
        "DATEMILLI" `(ArrowType$Date. DateUnit/MILLISECOND)

        "TIMENANO" `(ArrowType$Time. TimeUnit/NANOSECOND 64)
        "TIMEMICRO" `(ArrowType$Time. TimeUnit/MICROSECOND 64)
        "TIMEMILLI" `(ArrowType$Time. TimeUnit/MILLISECOND 32)
        "TIMESEC" `(ArrowType$Time. TimeUnit/SECOND 32)

        "INTERVALMONTHDAYNANO" `(ArrowType$Interval. IntervalUnit/MONTH_DAY_NANO)
        "INTERVALYEAR" `(ArrowType$Interval. IntervalUnit/YEAR_MONTH)
        "INTERVALDAY" `(ArrowType$Interval. IntervalUnit/DAY_TIME)

        ;; TODO there are other minor types that don't have a single corresponding ArrowType
        `(.getType ~(symbol (name 'org.apache.arrow.vector.types.Types$MinorType) minor-type-name))))))

(defn- return-types->field-type ^org.apache.arrow.vector.types.pojo.FieldType [return-types]
  (let [without-null (disj return-types types/null-type)]
    (case (count without-null)
      0 (FieldType. true types/null-type nil)
      1 (FieldType. (contains? return-types types/null-type)
                    (first without-null)
                    nil)
      (FieldType. false types/dense-union-type nil))))

(defn- write-value-out-code [^FieldType field-type return-types]
  (if-not (= types/dense-union-type (.getType field-type))
    {:write-value-out!
     (fn [^ArrowType arrow-type code]
       (when-not (= arrow-type types/null-type)
         (let [vec-type (types/arrow-type->vector-type arrow-type)]
           (set-value-form arrow-type
                           (-> `(.getVector ~out-writer-sym)
                               (with-tag vec-type))
                           `(.getPosition ~out-writer-sym)
                           code))))}

    (let [->writer-sym (->> return-types
                            (into {} (map (juxt identity (fn [_] (gensym 'writer))))))]
      {:writer-bindings
       (->> (cons [out-writer-sym `(.asDenseUnion ~out-writer-sym)]
                  (for [[arrow-type writer-sym] ->writer-sym]
                    [writer-sym `(.writerForType ~out-writer-sym
                                                 ;; HACK: pass leg-types through instead
                                                 (LegType. ~(arrow-type-literal-form arrow-type)))]))
            (apply concat))

       :write-value-out!
       (fn [^ArrowType arrow-type code]
         (let [writer-sym (->writer-sym arrow-type)
               vec-type (types/arrow-type->vector-type arrow-type)]
           `(do
              (.startValue ~writer-sym)
              ~(set-value-form arrow-type
                               (-> `(.getVector ~writer-sym)
                                   (with-tag vec-type))
                               `(.getPosition ~writer-sym)
                               code)
              (.endValue ~writer-sym))))})))

(defn batch-bindings [expr]
  (->> (walk/expr-seq expr)
       (mapcat :batch-bindings)
       (distinct)
       (apply concat)))

(defn- wrap-zone-id-cache-buster [f]
  (fn [expr opts]
    (f expr (assoc opts :zone-id (.getZone *clock*)))))

(def ^:private memo-generate-projection
  "NOTE: we macroexpand inside the memoize on the assumption that
   everything outside yields the same result on the pre-expanded expr - this
   assumption wouldn't hold if macroexpansion created new variable exprs, for example.
   macroexpansion is non-deterministic (gensym), so busts the memo cache."
  (-> (fn [expr opts]
        (let [expr (->> expr
                        (macro/macroexpand-all)
                        (walk/postwalk-expr (comp with-batch-bindings lit->param)))

              {:keys [return-types continue]} (codegen-expr expr opts)
              ret-field-type (return-types->field-type return-types)

              {:keys [writer-bindings write-value-out!]} (write-value-out-code ret-field-type return-types)]

          {:expr-fn (eval
                     `(fn [~(-> out-vec-sym (with-tag ValueVector))
                           ~(-> rel-sym (with-tag IIndirectRelation))
                           ~params-sym]
                        (let [~@(batch-bindings expr)

                              ~out-writer-sym (vw/vec->writer ~out-vec-sym)
                              ~@writer-bindings
                              row-count# (.rowCount ~rel-sym)]
                          (.setValueCount ~out-vec-sym row-count#)
                          (dotimes [~idx-sym row-count#]
                            (.startValue ~out-writer-sym)
                            ~(continue write-value-out!)
                            (.endValue ~out-writer-sym)))))

           :field-type ret-field-type}))
      (memoize)
      wrap-zone-id-cache-buster))

(defn field->value-types [^Field field]
  ;; potential duplication with LegType
  ;; probably doesn't work with structs, not convinced about lists either.
  (case (.name (Types/getMinorTypeForArrowType (.getType field)))
    "DENSEUNION"
    (mapv #(.getFieldType ^Field %) (.getChildren field))

    (.getFieldType field)))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-expr
  (fn [{:keys [op]} col-name {:keys [var-fields] :as opts}]
    op)
  :default ::default)

(defn field-with-name ^org.apache.arrow.vector.types.pojo.Field [^Field field, ^String col-name]
  (apply types/->field col-name (.getType field) (.isNullable field)
         (.getChildren field)))

(defmethod emit-expr :struct [{:keys [entries]} col-name opts]
  (let [entries (vec (for [[k v] entries]
                       (emit-expr v (name k) opts)))]

    (fn [^IIndirectRelation in-rel, al params]
      (let [evald-entries (LinkedList.)]
        (try
          (doseq [eval-entry entries]
            (.add evald-entries (eval-entry in-rel al params)))

          (let [row-count (.rowCount in-rel)

                ^Field out-field (apply types/->field col-name types/struct-type false
                                        (map #(.getField ^ValueVector %) evald-entries))

                ^StructVector out-vec (.createVector out-field al)]
            (try
              (.setValueCount out-vec row-count)

              (dotimes [idx row-count]
                (.setIndexDefined out-vec idx))

              (doseq [^ValueVector in-child-vec evald-entries
                      :let [out-child-vec (.getChild out-vec (.getName in-child-vec) ValueVector)]]
                (doto (.makeTransferPair in-child-vec out-child-vec)
                  (.splitAndTransfer 0 row-count)))

              out-vec
              (catch Throwable e
                (util/try-close out-vec)
                (throw e))))


          (finally
            (run! util/try-close evald-entries)))))))

(defmethod emit-expr :dot-const-field [{:keys [struct-expr field]} ^String col-name opts]
  (let [eval-expr (emit-expr struct-expr "dot" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector in-vec (eval-expr in-rel al params)]
        (let [out-field (types/->field col-name types/dense-union-type false)
              out-vec (.createVector out-field al)
              out-writer (vw/vec->writer out-vec)]
          (try
            (.setValueCount out-vec (.getValueCount in-vec))

            (let [col-rdr (-> (.structReader (iv/->direct-vec in-vec))
                              (.readerForKey (name field)))
                  copier (.rowCopier col-rdr out-writer)]

              (dotimes [idx (.getValueCount in-vec)]
                (.startValue out-writer)
                (.copyRow copier idx)
                (.endValue out-writer)))

            out-vec

            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(defmethod emit-expr :variable [{:keys [variable]} col-name {:keys [var-fields]}]
  (let [^Field out-field (-> (or (get var-fields variable)
                                 (throw (IllegalArgumentException. (str "missing variable: " variable ", available " (pr-str (set (keys var-fields)))))))
                             (field-with-name col-name))]
    (fn [^IIndirectRelation in-rel al _params]
      (let [in-vec (.vectorForName in-rel (name variable))
            out-vec (.createVector out-field al)]
        (.copyTo in-vec out-vec)
        (try
          out-vec
          (catch Throwable e
            (.close out-vec)
            (throw e)))))))

(defmethod emit-expr :list [{:keys [elements]} ^String col-name opts]
  (let [el-count (count elements)
        emitted-els (mapv #(emit-expr % "list-el" opts) elements)]
    (fn [^IIndirectRelation in-rel al params]
      (let [els (LinkedList.)]
        (try
          (doseq [eval-expr emitted-els]
            (.add els (eval-expr in-rel al params)))

          (let [out-vec (-> (types/->field col-name (ArrowType$FixedSizeList. el-count) false
                                           (types/->field "$data" types/dense-union-type false))
                            ^FixedSizeListVector (.createVector al))

                out-writer (.asList (vw/vec->writer out-vec))
                out-data-writer (.getDataWriter out-writer)]
            (try
              (let [child-row-count (.rowCount in-rel)
                    copiers (mapv (fn [res]
                                    (.rowCopier out-data-writer res))
                                  els)]
                (.setValueCount out-vec child-row-count)

                (dotimes [idx child-row-count]
                  (.setNotNull out-vec idx)
                  (.startValue out-writer)

                  (dotimes [el-idx el-count]
                    (.startValue out-data-writer)
                    (doto ^IRowCopier (nth copiers el-idx)
                      (.copyRow idx))
                    (.endValue out-data-writer))

                  (.endValue out-writer)))

              out-vec
              (catch Throwable e
                (.close out-vec)
                (throw e))))

          (finally
            (run! util/try-close els)))))))

(defmethod emit-expr :nth-const-n [{:keys [coll-expr ^long n]} col-name opts]
  (let [eval-coll (emit-expr coll-expr "nth-coll" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector coll-res (eval-coll in-rel al params)]
        (let [list-rdr (.listReader (iv/->direct-vec coll-res))
              coll-count (.getValueCount coll-res)
              out-vec (-> (types/->field col-name types/dense-union-type false)
                          (.createVector al))]
          (try
            (let [out-writer (vw/vec->writer out-vec)
                  copier (.elementCopier list-rdr out-writer)]

              (.setValueCount out-vec coll-count)

              (dotimes [idx coll-count]
                (.startValue out-writer)
                (.copyElement copier idx n)
                (.endValue out-writer))
              out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(defn- long-getter ^java.util.function.IntUnaryOperator [n-res]
  (condp = (class n-res)
    IntVector (reify IntUnaryOperator
                (applyAsInt [_ idx]
                  (.get ^IntVector n-res idx)))
    BigIntVector (reify IntUnaryOperator
                   (applyAsInt [_ idx]
                     (int (.get ^BigIntVector n-res idx))))))

(defmethod emit-expr :nth [{:keys [coll-expr n-expr]} col-name opts]
  (let [eval-coll (emit-expr coll-expr "nth-coll" opts)
        eval-n (emit-expr n-expr "nth-n" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector coll-res (eval-coll in-rel al params)
                  ^FieldVector n-res (eval-n in-rel al params)]
        (let [list-rdr (.listReader (iv/->direct-vec coll-res))
              coll-count (.getValueCount coll-res)
              out-vec (-> (types/->field col-name types/dense-union-type
                                         (or (.isNullable (.getField coll-res))
                                             (.isNullable (.getField n-res))))
                          (.createVector al))]
          (try
            (let [out-writer (vw/vec->writer out-vec)
                  copier (.elementCopier list-rdr out-writer)
                  !null-writer (delay (.writerForType (.asDenseUnion out-writer) LegType/NULL))]

              (.setValueCount out-vec coll-count)

              (let [->n (long-getter n-res)]
                (dotimes [idx coll-count]
                  (.startValue out-writer)
                  (if (.isNull n-res idx)
                    (doto ^IVectorWriter @!null-writer
                      (.startValue)
                      (.endValue))
                    (.copyElement copier idx (.applyAsInt ->n idx)))
                  (.endValue out-writer)))
              out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(def ^:private primitive-ops
  #{:variable :param :literal
    :call :let :local
    :if :if-some
    :metadata-field-present :metadata-vp-call})

(defn- emit-prim-expr [prim-expr ^String col-name {:keys [var-fields] :as opts}]
  (let [prim-expr (->> prim-expr
                       (walk/prewalk-expr (fn [{:keys [op] :as expr}]
                                            (if (contains? primitive-ops op)
                                              expr
                                              {:op :variable
                                               :variable (gensym 'var)
                                               :idx idx-sym
                                               :expr expr}))))
        var-sub-exprs (->> (walk/expr-seq prim-expr)
                           (filter #(= :variable (:op %))))
        emitted-sub-exprs (->> (for [{:keys [variable expr]} var-sub-exprs
                                     :when expr]
                                 (MapEntry/create variable
                                                  (emit-expr expr (name variable) opts)))
                               (into {}))]

    ;; TODO what are we going to do about needing to close over locals?
    (fn [^IIndirectRelation in-rel al params]
      (let [evald-sub-exprs (HashMap.)]
        (try
          (doseq [[variable eval-expr] emitted-sub-exprs]
            (.put evald-sub-exprs variable (eval-expr in-rel al params)))

          (let [var->types (->> (concat (for [[variable field] var-fields]
                                          (MapEntry/create variable (field->value-types field)))
                                        (for [[variable ^ValueVector out-vec] evald-sub-exprs]
                                          (MapEntry/create variable (field->value-types (.getField out-vec)))))
                                (into {}))

                {:keys [expr-fn ^FieldType field-type]} (memo-generate-projection prim-expr {:var->types var->types})
                out-field (Field. col-name field-type [])
                out-vec (.createVector out-field al)]

            (try
              (let [in-rel (-> (concat in-rel (for [[variable sub-expr-vec] evald-sub-exprs]
                                                (-> (iv/->direct-vec sub-expr-vec)
                                                    (.withName (name variable)))))
                               (iv/->indirect-rel (.rowCount in-rel)))]

                (expr-fn out-vec in-rel params))

              out-vec
              (catch Throwable e
                (.close out-vec)
                (throw e))))

          (finally
            (run! util/try-close (vals evald-sub-exprs))))))))

(doseq [op (-> (set (keys (methods codegen-expr)))
               (disj :variable))]
  (defmethod emit-expr op [prim-expr col-name opts]
    (emit-prim-expr prim-expr col-name opts)))

(defn ->var-fields [^IIndirectRelation in-rel, expr]
  (->> (for [var-sym (->> (walk/expr-seq expr)
                          (into #{} (comp (filter #(= :variable (:op %)))
                                          (map :variable))))]
         (let [^IIndirectVector ivec (or (.vectorForName in-rel (name var-sym))
                                         (throw (IllegalArgumentException. (str "missing read-col: " var-sym))))]
           (MapEntry/create var-sym (-> ivec (.getVector) (.getField)))))
       (into {})))

(defn ->expression-projection-spec ^core2.operator.IProjectionSpec [^String col-name form col-names params]
  (let [expr (form->expr form {:col-names col-names, :params params})]
    (reify IProjectionSpec
      (getColumnName [_] col-name)

      (project [_ allocator in-rel]
        (let [eval-expr (emit-expr expr col-name {:var-fields (->var-fields in-rel expr)})]
          (iv/->direct-vec (eval-expr in-rel allocator params)))))))

(defn ->expression-relation-selector ^core2.operator.IRelationSelector [form col-names params]
  (let [projector (->expression-projection-spec "select" (list 'boolean form) col-names params)]
    (reify IRelationSelector
      (select [_ al in-rel]
        (with-open [selection (.project projector al in-rel)]
          (let [^BitVector sel-vec (.getVector selection)
                res (IntStream/builder)]
            (dotimes [idx (.getValueCount selection)]
              (when (= 1 (.get sel-vec (.getIndex selection idx)))
                (.add res idx)))
            (.toArray (.build res))))))))

(defn eval-scalar-value [al form col-names params]
  (let [expr (form->expr form {:params params})]
    (case (:op expr)
      :literal form
      :param (get params (:param expr))

      ;; this is probably quite heavyweight to calculate a single value...
      (let [projection-spec (->expression-projection-spec "_scalar" form col-names params)]
        (with-open [out-vec (.project projection-spec al (iv/->indirect-rel [] 1))]
          (types/get-object (.getVector out-vec) (.getIndex out-vec 0)))))))
