(ns core2.expression
  (:require [clojure.set :as set]
            [core2.error :as err]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [clojure.string :as str])
  (:import (clojure.lang Keyword MapEntry)
           (core2 StringUtil)
           (core2.expression.boxes BoolBox DoubleBox LongBox ObjectBox)
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.types LegType)
           (core2.vector IIndirectRelation IIndirectVector IRowCopier IVectorWriter)
           (core2.vector.extensions KeywordType UuidType)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate Period ZoneId ZoneOffset ZonedDateTime)
           (java.time.temporal ChronoField ChronoUnit)
           (java.util Arrays Date HashMap LinkedList Map)
           (java.util.function IntUnaryOperator Function)
           (java.util.regex Pattern)
           (java.util.stream IntStream)
           (org.apache.arrow.vector BigIntVector BitVector DurationVector FieldVector IntVector IntervalDayVector IntervalYearVector PeriodDuration ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector StructVector)
           (org.apache.arrow.vector.types DateUnit IntervalUnit TimeUnit Types Types$MinorType)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Duration ArrowType$ExtensionType ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$Null ArrowType$Time ArrowType$Timestamp ArrowType$Utf8 Field FieldType)
           (org.apache.commons.codec.binary Hex)))

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

(defprotocol IBoxClass
  (->box-class [_]))

(extend-protocol IBoxClass
  ArrowType$Bool (->box-class [_] BoolBox)

  ;; TODO different box for different int-types
  ArrowType$Int (->box-class [_int-type] LongBox)

  ;; TODO different box for different float-types
  ArrowType$FloatingPoint (->box-class [_float-type] DoubleBox)

  ArrowType (->box-class [_] ObjectBox))

(defn- box-for [^Map boxes, ^ArrowType arrow-type]
  (.computeIfAbsent boxes arrow-type
                    (reify Function
                      (apply [_ arrow-type]
                        {:box-sym (gensym (types/type->field-name arrow-type))
                         :box-class (->box-class arrow-type)}))))

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

(defn- wrap-boxed-poly-return [{:keys [return-types continue] :as ret} {:keys [return-boxes]}]
  (let [mono? (= 1 (count return-types))]
    (-> ret
        (assoc :continue
               (if mono?
                 (fn [f]
                   (f (first return-types)
                      (continue (fn [_ code] code))))

                 (let [boxes (->> (for [rt return-types]
                                    [rt (box-for return-boxes rt)])
                                  (into {}))]
                   (fn [f]
                     (let [res-sym (gensym 'res)]
                       `(let [~res-sym
                              ~(continue (fn [return-type code]
                                           (let [{:keys [box-sym]} (get boxes return-type)]
                                             `(.withValue ~box-sym ~code))))]
                          (condp identical? ~res-sym
                            ~@(->> (for [[ret-type {:keys [box-sym]}] boxes]
                                     [box-sym (f ret-type `(.value ~box-sym))])
                                   (apply concat))))))))))))

(defmethod codegen-expr :variable [{:keys [variable idx], :or {idx idx-sym}} {:keys [var->types] :as opts}]
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

      (-> {:return-types
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
                         (apply concat)))))}

          (wrap-boxed-poly-return opts)))))

(defmethod codegen-expr :if [{:keys [pred then else]} opts]
  (let [p-boxes (HashMap.)
        {p-rets :return-types, p-cont :continue, :as emitted-p} (codegen-expr pred (assoc opts :return-boxes p-boxes))
        {t-rets :return-types, t-cont :continue, :as emitted-t} (codegen-expr then opts)
        {e-rets :return-types, e-cont :continue, :as emitted-e} (codegen-expr else opts)
        return-types (set/union t-rets e-rets)]
    (when-not (= #{ArrowType$Bool/INSTANCE} p-rets)
      (throw (IllegalArgumentException. (str "pred expression doesn't return boolean "
                                             (pr-str p-rets)))))

    (-> {:return-types return-types
         :boxes (into (vec (vals p-boxes)) (mapcat :boxes) [emitted-p emitted-t emitted-e])
         :continue (fn [f]
                     `(if ~(p-cont (fn [_ code] code))
                        ~(t-cont f)
                        ~(e-cont f)))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :local [{:keys [local]} {:keys [local-types]}]
  (let [return-type (get local-types local)]
    {:return-types #{return-type}
     :continue (fn [f] (f return-type local))}))

(defmethod codegen-expr :let [{:keys [local expr body]} opts]
  (let [expr-boxes (HashMap.)
        {continue-expr :continue, :as emitted-expr} (codegen-expr expr (assoc opts :return-boxes expr-boxes))
        emitted-bodies (->> (for [local-type (:return-types emitted-expr)]
                              (MapEntry/create local-type
                                               (codegen-expr body (assoc-in opts [:local-types local] local-type))))
                            (into {}))
        return-types (into #{} (mapcat :return-types) (vals emitted-bodies))]
    (-> {:return-types return-types
         :boxes (into (vec (vals expr-boxes)) (mapcat :boxes) (into [emitted-expr] (vals emitted-bodies)))
         :continue (fn [f]
                     (continue-expr (fn [local-type code]
                                      `(let [~local ~code]
                                         ~((:continue (get emitted-bodies local-type)) f)))))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :if-some [{:keys [local expr then else]} opts]
  (let [expr-boxes (HashMap.)

        {continue-expr :continue, expr-rets :return-types, :as emitted-expr}
        (codegen-expr expr (assoc opts :return-boxes expr-boxes))

        emitted-thens (->> (for [local-type (-> expr-rets
                                                (disj types/null-type))]
                             (MapEntry/create local-type
                                              (codegen-expr then (assoc-in opts [:local-types local] local-type))))
                           (into {}))
        then-rets (into #{} (mapcat :return-types) (vals emitted-thens))
        boxes (into (vec (vals expr-boxes)) (mapcat :boxes) (cons emitted-expr (vals emitted-thens)))]

    (-> (if-not (contains? expr-rets types/null-type)
          {:return-types then-rets
           :boxes boxes
           :continue (fn [f]
                       (continue-expr (fn [local-type code]
                                        `(let [~local ~code]
                                           ~((:continue (get emitted-thens local-type)) f)))))}

          (let [{e-rets :return-types, e-cont :continue, :as emitted-else} (codegen-expr else opts)
                return-types (into then-rets e-rets)]
            {:return-types return-types
             :boxes (into boxes (:boxes emitted-else))
             :continue (fn [f]
                         (continue-expr (fn [local-type code]
                                          (if (= local-type types/null-type)
                                            (e-cont f)
                                            `(let [~local ~code]
                                               ~((:continue (get emitted-thens local-type)) f))))))}))
        (wrap-boxed-poly-return opts))))

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

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-call*
  (fn [{:keys [f] :as expr}]
    (keyword f))
  :default ::default)

(def ^:private shortcut-null-args?
  (complement (comp #{:true? :false? :nil? :boolean
                      :compare-nulls-first :compare-nulls-last}
                    keyword)))

(defmethod codegen-call* ::default [{:keys [f emitted-args] :as expr}]
  (let [shortcut-null? (shortcut-null-args? f)

        all-arg-types (reduce (fn [acc {:keys [return-types]}]
                                (for [el acc
                                      return-type return-types]
                                  (conj el return-type)))
                              [[]]
                              emitted-args)

        emitted-calls (->> (for [arg-types all-arg-types
                                 :when (or (not shortcut-null?)
                                           (every? #(not= types/null-type %) arg-types))]
                             (MapEntry/create arg-types
                                              (codegen-call (assoc expr
                                                                   :args emitted-args
                                                                   :arg-types arg-types))))
                           (into {}))

        return-types (cond-> (->> emitted-calls
                                  (into #{} (mapcat (comp :return-types val))))
                       (and shortcut-null?
                            (some #(= types/null-type %)
                                  (sequence cat all-arg-types)))
                       (conj types/null-type))]


    {:return-types return-types
     :continue (fn continue-call-expr [handle-emitted-expr]
                 (let [build-args-then-call
                       (reduce (fn step [build-next-arg {continue-this-arg :continue}]
                                 ;; step: emit this arg, and pass it through to the inner build-fn
                                 (fn continue-building-args [arg-types emitted-args]
                                   (continue-this-arg (fn [arg-type emitted-arg]
                                                        (if (and shortcut-null? (= arg-type types/null-type))
                                                          (handle-emitted-expr types/null-type nil)
                                                          (build-next-arg (conj arg-types arg-type)
                                                                          (conj emitted-args emitted-arg)))))))

                               ;; innermost fn - we're done, call continue-call for these types
                               (fn call-with-built-args [arg-types emitted-args]
                                 (let [{:keys [continue-call]} (get emitted-calls arg-types)]
                                   (continue-call handle-emitted-expr emitted-args)))

                               ;; reverse because we're working inside-out
                               (reverse emitted-args))]
                   (build-args-then-call [] [])))}))

(defn- cont-b3-call [arg-type code]
  (if (= types/null-type arg-type)
    0
    `(long (if ~code 1 -1))))

(defmethod codegen-call* :and [{[{l-rets :return-types, l-cont :continue}
                                 {r-rets :return-types, r-cont :continue}] :emitted-args}]
  (let [return-types (set/union l-rets r-rets)]
    {:return-types return-types
     :continue (if-not (contains? return-types types/null-type)
                 (fn [f]
                   (f types/bool-type `(and ~(l-cont (fn [_ code] `(boolean ~code)))
                                            ~(r-cont (fn [_ code] `(boolean ~code))))))
                 (let [l-sym (gensym 'l)
                       r-sym (gensym 'r)]
                   (fn [f]
                     `(let [~l-sym ~(l-cont cont-b3-call)
                            ~r-sym (long (if (neg? ~l-sym)
                                           -1
                                           ~(r-cont cont-b3-call)))]
                        (if (zero? (min ~l-sym ~r-sym))
                          ~(f types/null-type nil)
                          ~(f types/bool-type `(== 1 ~l-sym ~r-sym)))))))}))

(defmethod codegen-call* :or [{[{l-rets :return-types, l-cont :continue}
                                {r-rets :return-types, r-cont :continue}] :emitted-args}]
  (let [return-types (set/union l-rets r-rets)]
    {:return-types return-types
     :continue (if-not (contains? return-types types/null-type)
                 (fn [f]
                   (f types/bool-type `(or ~(l-cont (fn [_ code] `(boolean ~code)))
                                           ~(r-cont (fn [_ code] `(boolean ~code))))))
                 (let [l-sym (gensym 'l)
                       r-sym (gensym 'r)]
                   (fn [f]
                     `(let [~l-sym ~(l-cont cont-b3-call)
                            ~r-sym (long (if (pos? ~l-sym)
                                           1
                                           ~(r-cont cont-b3-call)))]
                        (if (zero? (max ~l-sym ~r-sym))
                          ~(f types/null-type nil)
                          ~(f types/bool-type `(not (== -1 ~l-sym ~r-sym))))))))}))

(defmethod codegen-expr :call [{:keys [args] :as expr} opts]
  (let [emitted-args (for [arg args]
                       (let [boxes (HashMap.)]
                         (-> (codegen-expr arg (assoc opts :return-boxes boxes))
                             (update :boxes (fnil into []) (vals boxes)))))]
    (-> (codegen-call* (-> expr (assoc :emitted-args emitted-args)))
        (update :boxes (fnil into []) (mapcat :boxes) emitted-args)
        (wrap-boxed-poly-return opts))))

(defn mono-fn-call [return-type wrap-args-f]
  {:continue-call (fn [f emitted-args]
                    (f return-type (-> emitted-args wrap-args-f)))
   :return-types #{return-type}})

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

;; null-eq is an internal function used in situations where two nulls should compare equal,
;; e.g when grouping rows in group-by.
(defmethod codegen-call [:null-eq ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(= ~@%))))

(defmethod codegen-call [:null-eq ArrowType$Null ::types/Object] [_]
  (mono-fn-call types/bool-type (constantly false)))

(defmethod codegen-call [:null-eq ::types/Object ArrowType$Null] [_]
  (mono-fn-call types/bool-type (constantly false)))

(defmethod codegen-call [:null-eq ArrowType$Null ArrowType$Null] [_]
  (mono-fn-call types/bool-type (constantly true)))

(defmethod codegen-call [:<> ::types/Number ::types/Number] [_]
  (mono-fn-call types/bool-type #(do `(not (== ~@%)))))

(defmethod codegen-call [:<> ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(not= ~@%))))

(defmethod codegen-call [:not ArrowType$Bool] [_]
  (mono-fn-call types/bool-type #(do `(not ~@%))))

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

;; TODO extend min/max to non-numeric
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

(defmethod codegen-call [:double ::types/Number] [_]
  (mono-fn-call types/float8-type #(do `(double ~@%))))

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

(defmethod codegen-call [:like ArrowType$Binary ArrowType$Binary] [{[_ {:keys [literal]}] :args}]
  {:return-types #{types/bool-type}
   :continue-call (fn [f [haystack-code needle-code]]
                    (f types/bool-type
                       `(boolean (re-find ~(if literal
                                             (like->regex (binary->hex-like-pattern (resolve-bytes literal)))
                                             `(like->regex (binary->hex-like-pattern (buf->bytes ~needle-code))))
                                          (Hex/encodeHexString (buf->bytes ~haystack-code))))))})

(defmethod codegen-call [:like-regex ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8]
  [{:keys [args]}]
  (let [[_ re-literal flags] (map :literal args)

        flag-map
        {\s [Pattern/DOTALL]
         \i [Pattern/CASE_INSENSITIVE, Pattern/UNICODE_CASE]
         \m [Pattern/MULTILINE]}

        flag-int (int (reduce bit-or 0 (mapcat flag-map flags)))]

    {:return-types #{types/bool-type}
     :continue-call (fn [f [haystack-code needle-code]]
                      (f types/bool-type
                         `(boolean (re-find ~(if re-literal
                                               (Pattern/compile re-literal flag-int)
                                               `(Pattern/compile (resolve-string ~needle-code) ~flag-int))
                                            (resolve-string ~haystack-code)))))}))

;; apache commons has these functions but did not think they were worth the dep.
;; replace if we ever put commons on classpath.

(defn- sql-trim-leading
  [^String s ^String trim-char]
  (let [trim-cp (.codePointAt trim-char 0)]
    (loop [i 0]
      (if (< i (.length s))
        (let [cp (.codePointAt s i)]
          (if (= cp trim-cp)
            (recur (unchecked-add-int i (Character/charCount cp)))
            (.substring s i (.length s))))
        ""))))

(defn- sql-trim-trailing
  [^String s ^String trim-char]
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

(defn- trim-char-conforms?
  "Trim char is allowed to be length 1, or 2 - but only 2 if the 2 chars represent a single unicode character
  (but are split due to utf-16 surrogacy)."
  [^String trim-char]
  (or (= 1 (.length trim-char))
      (and (= 2 (.length trim-char))
           (= 2 (Character/charCount (.codePointAt trim-char 0))))))

(defn sql-trim
  "SQL Trim function.

  trim-spec is a string having one of the values: BOTH | TRAILING | LEADING.

  trim-char is a **SINGLE** unicode-character string e.g \" \". The string can include 2-char code points, as long as it is one
  logical unicode character.

  N.B trim-char is currently restricted to just one character, not all database implementations behave this way, in postgres you
  can specify multi-character strings for the trim char.

  See also sql-trim-leading, sql-trim-trailing"
  ^String [^String s ^String trim-spec ^String trim-char]
  (when-not (trim-char-conforms? trim-char) (throw (IllegalArgumentException. "Data Exception - trim error.")))
  (case trim-spec
    "LEADING" (sql-trim-leading s trim-char)
    "TRAILING" (sql-trim-trailing s trim-char)
    "BOTH" (-> s (sql-trim-leading trim-char) (sql-trim-trailing trim-char))))

(defmethod codegen-call [:trim ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8] [_]
  {:return-types #{types/varchar-type}
   :continue-call (fn [f [s trim-spec trim-char]]
                    (f types/varchar-type
                       `(ByteBuffer/wrap (.getBytes (sql-trim (resolve-string ~s) (resolve-string ~trim-spec) (resolve-string ~trim-char))
                                                    StandardCharsets/UTF_8))))})

(defn- binary-trim-leading
  [^bytes bin trim-octet]
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

(defn- binary-trim-trailing
  [^bytes bin trim-octet]
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

(defn binary-trim
  "SQL Trim function on binary.

  trim-spec is a string having one of the values: BOTH | TRAILING | LEADING.

  trim-octet is the byte value to trim.

  See also binary-trim-leading, binary-trim-trailing"
  [^bytes bin ^String trim-spec trim-octet]
  (case trim-spec
    "BOTH" (-> bin (binary-trim-leading trim-octet) (binary-trim-trailing trim-octet))
    "LEADING" (binary-trim-leading bin trim-octet)
    "TRAILING" (binary-trim-trailing bin trim-octet)))

(defmethod codegen-call [:trim ArrowType$Binary ArrowType$Utf8 ArrowType$Binary] [_]
  {:return-types #{types/varbinary-type}
   :continue-call (fn [f [s trim-spec trim-octet]]
                    (f types/varbinary-type
                       `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (first (resolve-bytes ~trim-octet))))))})

(defmethod codegen-call [:trim ArrowType$Binary ArrowType$Utf8 ::types/Number] [_]
  {:return-types #{types/varbinary-type}
   :continue-call (fn [f [s trim-spec trim-octet]]
                    (f types/varbinary-type
                       ;; should we throw an explicit error if no good cast to byte is possible?
                       `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (byte ~trim-octet)))))})

(defmethod codegen-call [:upper ArrowType$Utf8] [_]
  (mono-fn-call
    types/varchar-type
    (fn [[code]]
      `(ByteBuffer/wrap (.getBytes (.toUpperCase (resolve-string ~code)) StandardCharsets/UTF_8)))))

(defmethod codegen-call [:lower ArrowType$Utf8] [_]
  (mono-fn-call
    types/varchar-type
    (fn [[code]]
      `(ByteBuffer/wrap (.getBytes (.toLowerCase (resolve-string ~code)) StandardCharsets/UTF_8)))))

;; todo high perf variadic version, or specialisation for arity up to N.
(defmethod codegen-call [:concat ArrowType$Utf8 ArrowType$Utf8] [_]
  (mono-fn-call types/varchar-type (fn [[a b]]
                                     `(ByteBuffer/wrap (.getBytes (str (resolve-string ~a) (resolve-string ~b))
                                                                  StandardCharsets/UTF_8)))))

(defn bconcat ^bytes [^bytes b1 ^bytes b2]
  (let [ret (byte-array (+ (alength b1) (alength b2)))]
    (System/arraycopy b1 0 ret 0 (alength b1))
    (System/arraycopy b2 0 ret (alength b1) (alength b2))
    ret))

(defmethod codegen-call [:concat ArrowType$Binary ArrowType$Binary] [_]
  (mono-fn-call types/varbinary-type (fn [[a b]] `(ByteBuffer/wrap (bconcat (resolve-bytes ~a) (resolve-bytes ~b))))))

(defn resolve-utf8-buf ^ByteBuffer [s-or-buf]
  (if (instance? ByteBuffer s-or-buf)
    s-or-buf
    (ByteBuffer/wrap (.getBytes ^String s-or-buf StandardCharsets/UTF_8))))

(defmethod codegen-call [:substring ArrowType$Utf8 ArrowType$Int ArrowType$Int ArrowType$Bool] [_]
  {:return-types #{types/varchar-type}
   :continue-call (fn [f [x start length use-len]]
                    (f types/varchar-type `(StringUtil/sqlUtf8Substring (resolve-utf8-buf ~x) ~start ~length ~use-len)))})

(defmethod codegen-call [:substring ArrowType$Binary ArrowType$Int ArrowType$Int ArrowType$Bool] [_]
  {:return-types #{types/varbinary-type}
   :continue-call (fn [f [x start length use-len]]
                    (f types/varbinary-type `(StringUtil/sqlBinSubstring (resolve-buf ~x) ~start ~length ~use-len)))})

(defmethod codegen-call [:character-length ArrowType$Utf8 ArrowType$Utf8] [{:keys [args]}]
  (let [[_ unit] (map :literal args)]
    (mono-fn-call types/int-type (case unit
                                   "CHARACTERS" #(do `(StringUtil/utf8Length (resolve-utf8-buf ~(first %))))
                                   "OCTETS" #(do `(.remaining (resolve-utf8-buf ~(first %))))))))

(defmethod codegen-call [:octet-length ArrowType$Utf8] [_]
  (mono-fn-call types/int-type #(do `(.remaining (resolve-utf8-buf ~@%)))))

(defmethod codegen-call [:octet-length ArrowType$Binary] [_]
  (mono-fn-call types/int-type #(do `(.remaining (resolve-buf ~@%)))))

(defmethod codegen-call [:position ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8] [{:keys [args]}]
  (let [[_ _ unit] (map :literal args)]
    {:return-types #{types/int-type}
     :continue-call
     (fn [f [needle haystack]]
       (case unit
         "CHARACTERS"
         (f types/int-type `(StringUtil/sqlUtf8Position (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))
         "OCTETS"
         (f types/int-type `(StringUtil/SqlBinPosition (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))))}))

(defmethod codegen-call [:position ArrowType$Binary ArrowType$Binary] [_]
  (mono-fn-call types/int-type (fn [[needle haystack]] `(StringUtil/SqlBinPosition (resolve-buf ~needle) (resolve-buf ~haystack)))))

(defmethod codegen-call [:overlay ArrowType$Utf8 ArrowType$Utf8 ArrowType$Int ArrowType$Int] [_]
  (mono-fn-call
    types/varchar-type
    (fn [[target replacement from len]]
      `(StringUtil/sqlUtf8Overlay (resolve-utf8-buf ~target) (resolve-utf8-buf ~replacement) ~from ~len))))

(defmethod codegen-call [:overlay ArrowType$Binary ArrowType$Binary ArrowType$Int ArrowType$Int] [_]
  (mono-fn-call
    types/varbinary-type
    (fn [[target replacement from len]]
      `(StringUtil/sqlBinOverlay (resolve-buf ~target) (resolve-buf ~replacement) ~from ~len))))

(defmethod codegen-call [:default-overlay-length ArrowType$Utf8] [_]
  (mono-fn-call types/int-type #(do `(StringUtil/utf8Length (resolve-utf8-buf ~@%)))))

(defmethod codegen-call [:default-overlay-length ArrowType$Binary] [_]
  (mono-fn-call types/int-type #(do `(.remaining (resolve-buf ~@%)))))

(defn interval-eq
  "Override equality for intervals as we want 1 year to = 12 months, and this is not true by for Period.equals."
  ;; we might be able to enforce this differently (e.g all constructs, reads and calcs only use the month component).
  [^PeriodDuration pd1 ^PeriodDuration pd2]
  (let [p1 (.getPeriod pd1)
        p2 (.getPeriod pd2)]
    (and (= (.toTotalMonths p1) (.toTotalMonths p2))
         (= (.getDays p1) (.getDays p2))
         (= (.getDuration pd1) (.getDuration pd2)))))

;; interval equality specialisation, see interval-eq.
(defmethod codegen-call [:= ArrowType$Interval ArrowType$Interval] [_]
  (mono-fn-call types/bool-type #(do `(boolean (interval-eq ~@%)))))

(defn- ensure-interval-precision-valid [^long precision]
  (cond
    (< precision 1)
    (throw (IllegalArgumentException. "The minimum leading field precision is 1."))
    (< 8 precision)
    (throw (IllegalArgumentException. "The maximum leading field precision is 8."))))

(defn- ensure-interval-fractional-precision-valid [^long fractional-precision]
  (cond
    (< fractional-precision 0)
    (throw (IllegalArgumentException. "The minimum fractional seconds precision is 0."))
    (< 9 fractional-precision)
    (throw (IllegalArgumentException. "The maximum fractional seconds precision is 9."))))

(defmethod codegen-call [:single-field-interval ArrowType$Int ArrowType$Utf8 ArrowType$Int ArrowType$Int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit) (ensure-interval-fractional-precision-valid fractional-precision))
    (case unit
      "YEAR"
      (mono-fn-call types/interval-year-month-type #(do `(PeriodDuration. (Period/ofYears ~(first %)) Duration/ZERO)))
      "MONTH"
      (mono-fn-call types/interval-year-month-type #(do `(PeriodDuration. (Period/ofMonths ~(first %)) Duration/ZERO)))
      "DAY"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. (Period/ofDays ~(first %)) Duration/ZERO)))
      "HOUR"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (Duration/ofHours ~(first %)))))
      "MINUTE"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes ~(first %)))))
      "SECOND"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (Duration/ofSeconds ~(first %))))))))

(defn ensure-single-field-interval-int
  "Takes a string or UTF8 ByteBuffer and returns an integer, throws a parse error if the string does not contain an integer.

  This is used to parse INTERVAL literal strings, e.g INTERVAL '3' DAY, as the grammar has been overriden to emit a plain string."
  [string-or-buf]
  (try
    (Integer/valueOf (resolve-string string-or-buf))
    (catch NumberFormatException _
      (throw (IllegalArgumentException. "Parse error. Single field INTERVAL string must contain a positive or negative integer.")))))

(defn second-interval-fractional-duration
  "Takes a string or UTF8 ByteBuffer and returns Duration for a fractional seconds INTERVAL literal.

  e.g INTERVAL '3.14' SECOND

  Throws a parse error if the string does not contain an integer / decimal. Throws on overflow."
  ^Duration [string-or-buf]
  (try
    (let [s (resolve-string string-or-buf)
          bd (bigdec s)
          ;; will throw on overflow, is a custom error message needed?
          secs (.setScale bd 0 BigDecimal/ROUND_DOWN)
          nanos (.longValueExact (.setScale (.multiply (.subtract bd secs) 1e9M) 0 BigDecimal/ROUND_DOWN))]
      (Duration/ofSeconds (.longValueExact secs) nanos))
    (catch NumberFormatException _
      (throw (IllegalArgumentException. "Parse error. SECOND INTERVAL string must contain a positive or negative integer or decimal.")))))

(defmethod codegen-call [:single-field-interval ArrowType$Utf8 ArrowType$Utf8 ArrowType$Int ArrowType$Int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit) (ensure-interval-fractional-precision-valid fractional-precision))
    (case unit
      "YEAR"
      (mono-fn-call types/interval-year-month-type #(do `(PeriodDuration. (Period/ofYears (ensure-single-field-interval-int ~(first %))) Duration/ZERO)))
      "MONTH"
      (mono-fn-call types/interval-year-month-type #(do `(PeriodDuration. (Period/ofMonths (ensure-single-field-interval-int ~(first %))) Duration/ZERO)))
      "DAY"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. (Period/ofDays (ensure-single-field-interval-int ~(first %))) Duration/ZERO)))
      "HOUR"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (Duration/ofHours (ensure-single-field-interval-int ~(first %))))))
      "MINUTE"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes (ensure-single-field-interval-int ~(first %))))))
      "SECOND"
      (mono-fn-call types/interval-day-time-type #(do `(PeriodDuration. Period/ZERO (second-interval-fractional-duration ~(first %))))))))

(defn- parse-year-month-literal [s]
  (let [[match plus-minus part1 part2] (re-find #"^([-+]|)(\d+)\-(\d+)" s)]
    (when match
      (let [months (+ (* 12 (Integer/parseInt part1)) (Integer/parseInt part2))
            months' (if (= plus-minus "-") (- months) months)]
        (PeriodDuration. (Period/ofMonths months') Duration/ZERO)))))

(defn- fractional-secs-to->nanos ^long [fractional-secs]
  (if fractional-secs
    (let [num-digits (if (str/starts-with? fractional-secs "-")
                       (unchecked-dec-int (count fractional-secs))
                       (count fractional-secs))
          exp (- 9 num-digits)]
      (* (Math/pow 10 exp) (Long/parseLong fractional-secs)))
    0))

(defn- parse-day-to-second-literal [s unit1 unit2]
  (letfn [(negate-if-minus [plus-minus ^PeriodDuration pd]
            (if (= "-" plus-minus)
              (PeriodDuration.
                (.negated (.getPeriod pd))
                (.negated (.getDuration pd)))
              pd))]
    (case [unit1 unit2]
      ["DAY" "SECOND"]
      (let [re #"^([-+]|)(\d+) (\d+)\:(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus day hour min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                             (Duration/ofSeconds (+ (* 60 60 (Integer/parseInt hour))
                                                    (* 60 (Integer/parseInt min))
                                                    (Integer/parseInt sec))
                                                 (fractional-secs-to->nanos fractional-secs))))))
      ["DAY" "MINUTE"]
      (let [re #"^([-+]|)(\d+) (\d+)\:(\d+)$"
            [match plus-minus day hour min] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                             (Duration/ofMinutes (+ (* 60 (Integer/parseInt hour))
                                                    (Integer/parseInt min)))))))


      ["DAY" "HOUR"]
      (let [re #"^([-+]|)(\d+) (\d+)$"
            [match plus-minus day hour] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                             (Duration/ofHours (Integer/parseInt hour))))))

      ["HOUR" "SECOND"]
      (let [re #"^([-+]|)(\d+)\:(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus hour min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. Period/ZERO
                             (Duration/ofSeconds (+ (* 60 60 (Integer/parseInt hour))
                                                    (* 60 (Integer/parseInt min))
                                                    (Integer/parseInt sec))
                                                 (fractional-secs-to->nanos fractional-secs))))))

      ["HOUR" "MINUTE"]
      (let [re #"^([-+]|)(\d+)\:(\d+)$"
            [match plus-minus hour min] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. Period/ZERO
                             (Duration/ofMinutes (+ (* 60 (Integer/parseInt hour))
                                                    (Integer/parseInt min)))))))

      ["MINUTE" "SECOND"]
      (let [re #"^([-+]|)(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
            plus-minus
            (PeriodDuration. Period/ZERO
                             (Duration/ofSeconds (+ (* 60 (Integer/parseInt min))
                                                    (Integer/parseInt sec))
                                                 (fractional-secs-to->nanos fractional-secs)))))))))

(defn parse-multi-field-interval
  "This function is used to parse a 2 field interval literal into a PeriodDuration, e.g '12-03' YEAR TO MONTH."
  ^PeriodDuration [s unit1 unit2]
  ; This function overwhelming likely to be applied as a const-expr so not concerned about vectorized perf.

  ;; these rules are not strictly necessary
  ;; be are specified by SQL2011
  ;; if year, end field must be month.
  (when (= unit1 "YEAR")
    (when-not (= unit2 "MONTH")
      (throw (IllegalArgumentException. "If YEAR specified as the interval start field, MONTH must be the end field."))))

  ;; start cannot = month rule.
  (when (= unit1 "MONTH")
    (throw (IllegalArgumentException. "MONTH is not permitted as the interval start field.")))

  ;; less significance rule.
  (when-not (or (= unit1 "YEAR")
                (and (= unit1 "DAY") (#{"HOUR" "MINUTE" "SECOND"} unit2))
                (and (= unit1 "HOUR") (#{"MINUTE" "SECOND"} unit2))
                (and (= unit1 "MINUTE") (#{"SECOND"} unit2)))
    (throw (IllegalArgumentException. "Interval end field must have less significance than the start field.")))

  (or (if (= "YEAR" unit1)
        (parse-year-month-literal s)
        (parse-day-to-second-literal s unit1 unit2))
      (throw (IllegalArgumentException. "Cannot parse interval, incorrect format."))))

(defmethod codegen-call [:multi-field-interval ArrowType$Utf8 ArrowType$Utf8 ArrowType$Int ArrowType$Utf8 ArrowType$Int] [{:keys [args]}]
  (let [[_ unit1 precision unit2 fractional-precision] (map :literal args)
        _ (ensure-interval-precision-valid precision)
        _ (when (= "SECOND" unit2) (ensure-interval-fractional-precision-valid fractional-precision))
        ;; todo choose a more specific representation when possible
        return-type types/interval-month-day-nano-type]
    (mono-fn-call return-type (fn [[s & _]] `(parse-multi-field-interval (resolve-string ~s) ~unit1 ~unit2)))))

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
  (defmethod codegen-call [math-op ::types/Number] [_]
    {:return-types #{types/float8-type}
     :continue-call (fn [f emitted-args]
                      (f types/float8-type
                         `(~math-method ~@emitted-args)))}))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti set-value-form
  (fn [arrow-type out-vec-sym idx-sym code]
    (class arrow-type)))

(defmethod set-value-form ArrowType$Null [_ _out-vec-sym _idx-sym _code])

(defmethod set-value-form ArrowType$Bool [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (if ~code 1 0)))

(defmethod set-value-form ArrowType$Int [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (long ~code)))

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

(defmethod set-value-form ArrowType$Interval [^ArrowType$Interval t out-vec-sym idx-sym code]
  (util/case-enum (.getUnit t)
    IntervalUnit/YEAR_MONTH
    `(let [^PeriodDuration period-duration# ~code
           period# (.getPeriod period-duration#)]
       (.set ^IntervalYearVector ~out-vec-sym ~idx-sym (.toTotalMonths period#)))
    IntervalUnit/DAY_TIME
    `(let [^PeriodDuration period-duration# ~code
           period# (.getPeriod period-duration#)
           duration# (.getDuration period-duration#)
           ddays# (.toDaysPart duration#)
           dsecs# (- (.getSeconds duration#) (* ddays# 86400))
           dmillis# (.toMillisPart duration#)]
       (.set ^IntervalDayVector ~out-vec-sym ~idx-sym (+ (.getDays period#) ddays#) (+ (* dsecs# 1000) dmillis#)))
    IntervalUnit/MONTH_DAY_NANO
    `(let [^PeriodDuration period-duration# ~code
           period# (.getPeriod period-duration#)
           duration# (.getDuration period-duration#)
           ddays# (.toDaysPart duration#)
           dsecs# (- (.getSeconds duration#) (* ddays# 86400))
           dnanos# (.toNanosPart duration#)]
       (.set ~out-vec-sym ~idx-sym (.toTotalMonths period#) (+ (.getDays period#) ddays#) (+ (* dsecs# 1000000000) dnanos#)))))

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
      0 (FieldType/nullable types/null-type)
      1 (FieldType. (contains? return-types types/null-type)
                    (first without-null)
                    nil)
      (FieldType/notNullable types/dense-union-type))))

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

(defn box-bindings [boxes]
  (->> (for [{:keys [box-sym box-class]} boxes]
         [box-sym `(new ~box-class)])
       (apply concat)))

(def ^:private memo-generate-projection
  "NOTE: we macroexpand inside the memoize on the assumption that
   everything outside yields the same result on the pre-expanded expr - this
   assumption wouldn't hold if macroexpansion created new variable exprs, for example.
   macroexpansion is non-deterministic (gensym), so busts the memo cache."
  (-> (fn [expr opts]
        (let [expr (->> expr
                        (macro/macroexpand-all)
                        (walk/postwalk-expr (comp with-batch-bindings lit->param)))

              return-boxes (HashMap.)
              {:keys [return-types continue boxes]} (codegen-expr expr (-> opts (assoc :return-boxes return-boxes)))
              ret-field-type (return-types->field-type return-types)

              {:keys [writer-bindings write-value-out!]} (write-value-out-code ret-field-type return-types)]

          {:expr-fn (-> `(fn [~(-> out-vec-sym (with-tag ValueVector))
                              ~(-> rel-sym (with-tag IIndirectRelation))
                              ~params-sym]
                           (let [~@(batch-bindings expr)

                                 ~@(box-bindings (concat boxes (vals return-boxes)))

                                 ~out-writer-sym (vw/vec->writer ~out-vec-sym)
                                 ~@writer-bindings

                                 row-count# (.rowCount ~rel-sym)]
                             (.setValueCount ~out-vec-sym row-count#)
                             (dotimes [~idx-sym row-count#]
                               (.startValue ~out-writer-sym)
                               ~(continue write-value-out!)
                               (.endValue ~out-writer-sym))))
                        #_(doto clojure.pprint/pprint)
                        eval)

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
