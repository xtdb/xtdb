(ns core2.expression
  (:require [clojure.core.match :refer [match]]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (clojure.lang Keyword MapEntry)
           (core2 StringUtil)
           (core2.expression.boxes BoolBox DoubleBox LongBox ObjectBox)
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.vector IIndirectRelation IIndirectVector IRowCopier IVectorWriter)
           (core2.vector.extensions KeywordType UuidType)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate Period ZoneId ZoneOffset ZonedDateTime)
           (java.time.temporal ChronoField ChronoUnit)
           (java.util Arrays Date HashMap LinkedList Map)
           (java.util.function Function IntUnaryOperator)
           (java.util.regex Pattern)
           (java.util.stream IntStream)
           (org.apache.arrow.vector BigIntVector BitVector DurationVector IntVector IntervalDayVector IntervalYearVector NullVector PeriodDuration ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector)
           (org.apache.arrow.vector.types FloatingPointPrecision Types)
           (org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$List ArrowType$Union Field FieldType)
           (org.apache.commons.codec.binary Hex)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti parse-list-form
  (fn [[f & args] env]
    f)
  :default ::default)

(defn form->expr [form {:keys [col-types param-types locals] :as env}]
  (cond
    (symbol? form) (cond
                     (contains? locals form) {:op :local, :local form}
                     (contains? param-types form) {:op :param, :param form, :param-type (get param-types form)}
                     (contains? col-types form) {:op :variable, :variable form, :var-type (get col-types form)}
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

(defmethod parse-list-form '. [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'.' expects 2 args: " (pr-str form)))))

  (let [[struct field] args]
    (when-not (symbol? field)
      (throw (IllegalArgumentException. (str "'.' expects symbol fields: " (pr-str form)))))

    {:op :vectorised-call
     :f :get-field
     :args [(form->expr struct env)]
     :field field}))

(defmethod parse-list-form '.. [[_ & args :as form] env]
  (let [[struct & fields] args]
    (when-not (seq fields)
      (throw (IllegalArgumentException. (str "'..' expects at least 2 args: " (pr-str form)))))
    (when-not (every? symbol? fields)
      (throw (IllegalArgumentException. (str "'..' expects symbol fields: " (pr-str form)))))
    (reduce (fn [struct-expr field]
              {:op :vectorised-call
               :f :get-field
               :args [struct-expr]
               :field field})
            (form->expr struct env)
            (rest args))))

(defmethod parse-list-form 'nth [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'nth' expects 2 args: " (pr-str form)))))

  (let [[coll-form n-form] args]
    (if (integer? n-form)
      {:op :vectorised-call
       :f :nth-const-n
       :args [(form->expr coll-form env)]
       :n n-form}
      {:op :vectorised-call
       :f :nth
       :args [(form->expr coll-form env)
              (form->expr n-form env)]})))

(defmethod parse-list-form 'trim-array [[_ arr n] env]
  {:op :vectorised-call
   :f :trim-array
   :args [(form->expr arr env)
          (form->expr n env)]})

(defmethod parse-list-form 'cast [[_ expr cast-type] env]
  {:op :call
   :f :cast
   :args [(form->expr expr env)]
   :cast-type cast-type})

(defmethod parse-list-form ::default [[f & args] env]
  {:op :call, :f f, :args (mapv #(form->expr % env) args)})

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
         :duration 'long}
        types/col-type-head))

(def idx-sym (gensym 'idx))
(def rel-sym (gensym 'rel))
(def params-sym (gensym 'params))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti with-batch-bindings
  (fn [expr opts]
    (:op expr))
  :default ::default)

(defmethod with-batch-bindings ::default [expr _] expr)

(defmulti ->box-class types/col-type-head, :hierarchy #'types/col-type-hierarchy)

(defmethod ->box-class :bool [_] BoolBox)
(defmethod ->box-class :int [_] LongBox) ; TODO different box for different int-types
(defmethod ->box-class :float [_] DoubleBox) ; TODO different box for different float-types

(doseq [k #{:timestamp-tz :timestamp-local :date :time :duration}]
  (defmethod ->box-class k [_] LongBox))

(defmethod ->box-class :any [_] ObjectBox)

(defn- box-for [^Map boxes, col-type]
  (.computeIfAbsent boxes col-type
                    (reify Function
                      (apply [_ col-type]
                        {:box-sym (gensym (types/col-type->field-name col-type))
                         :box-class (->box-class col-type)}))))

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
  (let [return-type (types/value->col-type literal)
        literal-type (class literal)]
    {:return-type return-type
     :continue (fn [f]
                 (f return-type (emit-value literal-type literal)))
     :literal literal}))

(defn lit->param [{:keys [op] :as expr}]
  (if (= op :literal)
    (let [{:keys [literal]} expr]
      {:op :param, :param (gensym 'lit),
       :param-type (types/value->col-type literal)
       :param-class (class literal)
       :literal literal})
    expr))

(defmethod with-batch-bindings :param [{:keys [param] :as expr} {:keys [param-classes]}]
  (let [param-class (get expr :param-class (get param-classes param))]
    (-> expr
        (assoc :batch-bindings
               [[param
                 (emit-value param-class
                             (get expr :literal
                                  (cond-> `(get ~params-sym '~param)
                                    param-class (with-tag param-class))))]]))))

(defmethod codegen-expr :param [{:keys [param param-type] :as expr} _]
  (into {:return-type param-type
         :continue (fn [f]
                     (f param-type param))}
        (select-keys expr #{:literal})))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti get-value-form
  (fn [value-type vec-sym idx-sym]
    (types/col-type-head value-type))
  :default ::default
  :hierarchy #'types/col-type-hierarchy)

(defmethod get-value-form :null [_ _ _] nil)
(defmethod get-value-form :bool [_ vec-sym idx-sym] `(= 1 (.get ~vec-sym ~idx-sym)))
(defmethod get-value-form :float [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form :int [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form :timestamp-tz [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form :timestamp-local [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form :duration [_ vec-sym idx-sym] `(DurationVector/get (.getDataBuffer ~vec-sym) ~idx-sym))
(defmethod get-value-form :utf8 [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form :varbinary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form :fixed-size-binary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))

;; will unify both possible vector representations
;; as an epoch int, do not think it is worth branching for both cases in all date functions.
(defmethod get-value-form :date [[_ date-unit] vec-sym idx-sym]
  (case date-unit
    :day `(.get ~vec-sym ~idx-sym)
    :milli `(int (quot (.get ~vec-sym ~idx-sym) 86400000))))

;; unifies to nanos (of day) long
(defmethod get-value-form :time [[_ time-unit] vec-sym idx-sym]
  (case time-unit
    :nano `(.get ~vec-sym ~idx-sym)
    :micro `(* 1e3 (.get ~vec-sym ~idx-sym))
    :milli `(* 1e6 (long (get ~vec-sym ~idx-sym)))
    :second `(* 1e9 (long (get ~vec-sym ~idx-sym)))))

;; we will box for simplicity given bigger than a long in worst case, may later change to a more primitive
(defmethod get-value-form :interval [_type vec-sym idx-sym]
  `(types/get-object ~vec-sym ~idx-sym))

(defmethod get-value-form :extension-type [[_ _ underlying-type _] vec-sym idx-sym]
  ;; a reasonable default, but implement it for more specific types if this doesn't work
  (get-value-form underlying-type `(.getUnderlyingVector ~vec-sym) idx-sym))

(defmethod with-batch-bindings :variable [{:keys [variable] :as expr} _]
  (-> expr
      (assoc :batch-bindings
             [[(-> variable (with-tag IIndirectVector))
               `(.vectorForName ~rel-sym ~(name variable))]])))

(defn- wrap-boxed-poly-return [{:keys [return-type continue] :as ret} {:keys [return-boxes]}]
  (-> ret
      (assoc :continue (match return-type
                         [:union inner-types]
                         (let [boxes (->> (for [rt inner-types]
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
                                           (apply concat)))))))

                         :else
                         (fn [f]
                           (f return-type (continue (fn [_ code] code))))))))

(defmethod codegen-expr :variable [{:keys [variable idx], :or {idx idx-sym}} {:keys [var->types var->col-type]}]
  ;; NOTE we now get the widest var-type in the expr itself, but don't use it here (yet? at all?)
  (let [col-type (or (get var->col-type variable)
                     (throw (AssertionError. (str "unknown variable: " variable))))
        field-types (or (get var->types variable)
                        (throw (AssertionError. (str "unknown variable: " variable))))
        var-idx-sym (gensym 'var-idx)
        var-vec-sym (gensym 'var-vec)]
    {:return-type col-type
     :continue (fn [f]
                 ;; HACK this wants to use col-types throughout, really...
                 (letfn [(cont [^FieldType field-type]
                           (let [arrow-type (.getType field-type)
                                 col-type (types/arrow-type->col-type arrow-type)
                                 cont-nn (f col-type (get-value-form col-type var-vec-sym var-idx-sym))]
                             (if (.isNullable field-type)
                               `(if (.isNull ~var-vec-sym ~var-idx-sym)
                                  ~(f :null nil)
                                  ~cont-nn)
                               cont-nn)))]
                   (if (coll? field-types)
                     `(let [~var-idx-sym (.getIndex ~variable ~idx)
                            ~(-> var-vec-sym (with-tag DenseUnionVector)) (.getVector ~variable)]
                        (case (.getTypeId ~var-vec-sym ~var-idx-sym)
                          ~@(->> field-types
                                 (sequence
                                  (comp
                                   (map-indexed
                                    (fn [type-id ^FieldType field-type]
                                      [type-id
                                       (let [arrow-type (.getType field-type)]
                                         `(let [~var-idx-sym (.getOffset ~var-vec-sym ~var-idx-sym)

                                                ~(-> var-vec-sym (with-tag (types/arrow-type->vector-type arrow-type)))
                                                (.getVectorByType ~var-vec-sym ~type-id)]

                                            ~(cont field-type)))]))
                                   cat)))))

                     (let [^FieldType field-type field-types
                           arrow-type (.getType field-type)]
                       `(let [~(-> var-vec-sym (with-tag (types/arrow-type->vector-type arrow-type))) (.getVector ~variable)
                              ~var-idx-sym (.getIndex ~variable ~idx)]
                          ~(cont field-type))))))}))

(defmethod codegen-expr :if [{:keys [pred then else]} opts]
  (let [p-boxes (HashMap.)
        {p-ret :return-type, p-cont :continue, :as emitted-p} (codegen-expr pred (assoc opts :return-boxes p-boxes))
        {t-ret :return-type, t-cont :continue, :as emitted-t} (codegen-expr then opts)
        {e-ret :return-type, e-cont :continue, :as emitted-e} (codegen-expr else opts)
        return-type (types/merge-col-types t-ret e-ret)]
    (when-not (= :bool p-ret)
      (throw (IllegalArgumentException. (str "pred expression doesn't return boolean "
                                             (pr-str p-ret)))))

    (-> {:return-type return-type
         :boxes (into (vec (vals p-boxes)) (mapcat :boxes) [emitted-p emitted-t emitted-e])
         :continue (fn [f]
                     `(if ~(p-cont (fn [_ code] code))
                        ~(t-cont f)
                        ~(e-cont f)))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :local [{:keys [local]} {:keys [local-types]}]
  (let [return-type (get local-types local)]
    {:return-type return-type
     :continue (fn [f] (f return-type local))}))

(defmethod codegen-expr :let [{:keys [local expr body]} opts]
  (let [expr-boxes (HashMap.)
        {local-type :return-type, continue-expr :continue, :as emitted-expr} (codegen-expr expr (assoc opts :return-boxes expr-boxes))
        emitted-bodies (->> (for [local-type (types/flatten-union-types local-type)]
                              (MapEntry/create local-type
                                               (codegen-expr body (assoc-in opts [:local-types local] local-type))))
                            (into {}))
        return-type (apply types/merge-col-types (into #{} (map :return-type) (vals emitted-bodies)))]
    (-> {:return-type return-type
         :boxes (into (vec (vals expr-boxes)) (mapcat :boxes) (into [emitted-expr] (vals emitted-bodies)))
         :continue (fn [f]
                     (continue-expr (fn [local-type code]
                                      `(let [~local ~code]
                                         ~((:continue (get emitted-bodies local-type)) f)))))}
        (wrap-boxed-poly-return opts))))

(defmethod codegen-expr :if-some [{:keys [local expr then else]} opts]
  (let [expr-boxes (HashMap.)

        {continue-expr :continue, expr-ret :return-type, :as emitted-expr}
        (codegen-expr expr (assoc opts :return-boxes expr-boxes))

        expr-rets (types/flatten-union-types expr-ret)

        emitted-thens (->> (for [local-type (disj expr-rets :null)]
                             (MapEntry/create local-type
                                              (codegen-expr then (assoc-in opts [:local-types local] local-type))))
                           (into {}))
        then-rets (into #{} (map :return-type) (vals emitted-thens))
        then-ret (apply types/merge-col-types then-rets)

        boxes (into (vec (vals expr-boxes)) (mapcat :boxes) (cons emitted-expr (vals emitted-thens)))]

    (-> (if-not (contains? expr-rets :null)
          {:return-type then-ret
           :boxes boxes
           :continue (fn [f]
                       (continue-expr (fn [local-type code]
                                        `(let [~local ~code]
                                           ~((:continue (get emitted-thens local-type)) f)))))}

          (let [{e-ret :return-type, e-cont :continue, :as emitted-else} (codegen-expr else opts)]
            {:return-type (apply types/merge-col-types e-ret then-rets)
             :boxes (into boxes (:boxes emitted-else))
             :continue (fn [f]
                         (continue-expr (fn [local-type code]
                                          (if (= local-type :null)
                                            (e-cont f)
                                            `(let [~local ~code]
                                               ~((:continue (get emitted-thens local-type)) f))))))}))
        (wrap-boxed-poly-return opts))))

(defmulti codegen-mono-call
  "Expects a map containing both the expression and an `:arg-types` key - a vector of col-types.
   This `:arg-types` vector should be monomorphic - if the args are polymorphic, call this multimethod multiple times.

   Returns a map containing
    * `:return-types` (set)
    * `:->call-code` (fn [emitted-args])"

  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (name f))
               (map types/col-type-head arg-types))))

  :hierarchy #'types/col-type-hierarchy)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-call
  (fn [{:keys [f] :as expr}]
    (keyword f))
  :default ::default)

(def ^:private shortcut-null-args?
  (complement (comp #{:true? :false? :nil? :boolean
                      :compare-nulls-first :compare-nulls-last}
                    keyword)))

(defmethod codegen-call ::default [{:keys [f emitted-args] :as expr}]
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
                                              (codegen-mono-call (assoc expr
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
                                 (let [{:keys [return-type ->call-code]} (get emitted-calls arg-types)]
                                   (handle-emitted-expr return-type (->call-code emitted-args))))

                               ;; reverse because we're working inside-out
                               (reverse emitted-args))]
                   (build-args-then-call [] [])))}))

(defn- cont-b3-call [arg-type code]
  (if (= :null arg-type)
    0
    `(long (if ~code 1 -1))))

(defn- nullable? [col-type]
  (contains? (types/flatten-union-types col-type) :null))

(defmethod codegen-call :and [{[{l-ret :return-type, l-cont :continue}
                                {r-ret :return-type, r-cont :continue}] :emitted-args}]
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

(defmethod codegen-call :or [{[{l-ret :return-type, l-cont :continue}
                               {r-ret :return-type, r-cont :continue}] :emitted-args}]
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

(defmethod codegen-expr :call [{:keys [args] :as expr} opts]
  (let [emitted-args (for [arg args]
                       (let [boxes (HashMap.)]
                         (-> (codegen-expr arg (assoc opts :return-boxes boxes))
                             (update :boxes (fnil into []) (vals boxes)))))]
    (-> (codegen-call (-> expr (assoc :emitted-args emitted-args)))
        (update :boxes (fnil into []) (mapcat :boxes) emitted-args)
        (wrap-boxed-poly-return opts))))

(doseq [[f-kw cmp] [[:< #(do `(neg? ~%))]
                    [:<= #(do `(not (pos? ~%)))]
                    [:> #(do `(pos? ~%))]
                    [:>= #(do `(not (neg? ~%)))]]
        :let [f-sym (symbol (name f-kw))]]

  (doseq [col-type #{:num :timestamp-tz :duration :date}]
    (defmethod codegen-mono-call [f-kw col-type col-type] [_]
      {:return-type :bool, :->call-code #(do `(~f-sym ~@%))}))

  (doseq [col-type #{:varbinary :utf8}]
    (defmethod codegen-mono-call [f-kw col-type col-type] [_]
      {:return-type :bool, :->call-code #(cmp `(util/compare-nio-buffers-unsigned ~@%))}))

  (defmethod codegen-mono-call [f-kw :any :any] [_]
    {:return-type :bool, :->call-code #(cmp `(compare ~@%))}))

(defmethod codegen-mono-call [:= :num :num] [_]
  {:return-type :bool
   :->call-code #(do `(== ~@%))})

(defmethod codegen-mono-call [:= :any :any] [_]
  {:return-type :bool
   :->call-code #(do `(= ~@%))})

;; null-eq is an internal function used in situations where two nulls should compare equal,
;; e.g when grouping rows in group-by.
(defmethod codegen-mono-call [:null-eq :any :any] [_]
  {:return-type :bool, :->call-code #(do `(= ~@%))})

(defmethod codegen-mono-call [:null-eq :null :any] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-mono-call [:null-eq :any :null] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-mono-call [:null-eq :null :null] [_]
  {:return-type :bool, :->call-code (constantly true)})

(defmethod codegen-mono-call [:<> :num :num] [_]
  {:return-type :bool, :->call-code #(do `(not (== ~@%)))})

(defmethod codegen-mono-call [:<> :any :any] [_]
  {:return-type :bool, :->call-code #(do `(not= ~@%))})

(defmethod codegen-mono-call [:not :bool] [_]
  {:return-type :bool, :->call-code #(do `(not ~@%))})

(defmethod codegen-mono-call [:true? :bool] [_]
  {:return-type :bool, :->call-code #(do `(true? ~@%))})

(defmethod codegen-mono-call [:false? :bool] [_]
  {:return-type :bool, :->call-code #(do `(false? ~@%))})

(defmethod codegen-mono-call [:nil? :null] [_]
  {:return-type :bool, :->call-code (constantly true)})

(doseq [f #{:true? :false? :nil?}]
  (defmethod codegen-mono-call [f :any] [_]
    {:return-type :bool, :->call-code (constantly false)}))

(defmethod codegen-mono-call [:boolean :null] [_]
  {:return-type :bool, :->call-code (constantly false)})

(defmethod codegen-mono-call [:boolean :bool] [_]
  {:return-type :bool, :->call-code first})

(defmethod codegen-mono-call [:boolean :any] [_]
  {:return-type :bool, :->call-code (constantly true)})

(defn- with-math-integer-cast
  "java.lang.Math's functions only take int or long, so we introduce an up-cast if need be"
  [int-type emitted-args]
  (let [arg-cast (if (isa? types/widening-hierarchy int-type :i32) 'int 'long)]
    (map #(list arg-cast %) emitted-args)))

(defmethod codegen-mono-call [:+ :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound* arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/addExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:+ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(+ ~@%))})

(defmethod codegen-mono-call [:- :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound* arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:- :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(- ~@%))})

(defmethod codegen-mono-call [:- :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  (list (type->cast x-type)
                        `(Math/negateExact ~@(with-math-integer-cast x-type emitted-args))))})

(defmethod codegen-mono-call [:- :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code #(do `(- ~@%))})

(defmethod codegen-mono-call [:* :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound* arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/multiplyExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:* :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(* ~@%))})

(defmethod codegen-mono-call [:mod :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(mod ~@%))})

(defmethod codegen-mono-call [:/ :int :int] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(quot ~@%))})

(defmethod codegen-mono-call [:/ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(/ ~@%))})

;; TODO extend min/max to non-numeric
(defmethod codegen-mono-call [:max :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(Math/max ~@%))})

(defmethod codegen-mono-call [:min :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound* arg-types)
   :->call-code #(do `(Math/min ~@%))})

(defmethod codegen-mono-call [:power :num :num] [_]
  {:return-type :f64, :->call-code #(do `(Math/pow ~@%))})

(defmethod codegen-mono-call [:log :num :num] [_]
  {:return-type :f64,
   :->call-code (fn [[base x]]
                  `(/ (Math/log ~x) (Math/log ~base)))})

(defmethod codegen-mono-call [:double :num] [_]
  {:return-type :f64, :->call-code #(do `(double ~@%))})

(defn like->regex [like-pattern]
  (-> like-pattern
      (Pattern/quote)
      (.replace "%" "\\E.*\\Q")
      (.replace "_" "\\E.\\Q")
      (->> (format "^%s\\z"))
      re-pattern))

(defmethod codegen-mono-call [:like :utf8 :utf8] [{[_ {:keys [literal]}] :args}]
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

(defmethod codegen-mono-call [:like :varbinary :varbinary] [{[_ {:keys [literal]}] :args}]
  {:return-type :bool
   :->call-code (fn [[haystack-code needle-code]]
                  `(boolean (re-find ~(if literal
                                        (like->regex (binary->hex-like-pattern (resolve-bytes literal)))
                                        `(like->regex (binary->hex-like-pattern (buf->bytes ~needle-code))))
                                     (Hex/encodeHexString (buf->bytes ~haystack-code)))))})

(defmethod codegen-mono-call [:like-regex :utf8 :utf8 :utf8] [{:keys [args]}]
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

(defmethod codegen-mono-call [:trim :utf8 :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s trim-spec trim-char]]
                  `(ByteBuffer/wrap (.getBytes (sql-trim (resolve-string ~s) (resolve-string ~trim-spec) (resolve-string ~trim-char))
                                               StandardCharsets/UTF_8)))})

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

(defmethod codegen-mono-call [:trim :varbinary :utf8 :varbinary] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-spec trim-octet]]
                  `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (first (resolve-bytes ~trim-octet)))))})

(defmethod codegen-mono-call [:trim :varbinary :utf8 :num] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-spec trim-octet]]
                  ;; should we throw an explicit error if no good cast to byte is possible?
                  `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (byte ~trim-octet))))})

(defmethod codegen-mono-call [:upper :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[code]]
                  `(ByteBuffer/wrap (.getBytes (.toUpperCase (resolve-string ~code)) StandardCharsets/UTF_8)))})

(defmethod codegen-mono-call [:lower :utf8] [_]
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
  (let [out (allocate-concat-out-buffer bufs)
        ;; this does not reset position of input bufs, but it should be ok to consume them, if caller cares, it is possible
        ;; to reset their positions.
        _ (reduce (fn [^ByteBuffer out ^ByteBuffer buf] (.put out buf) out) out bufs)]
    (.position out 0)
    out))

(defn resolve-utf8-buf ^ByteBuffer [s-or-buf]
  (if (instance? ByteBuffer s-or-buf)
    s-or-buf
    (ByteBuffer/wrap (.getBytes ^String s-or-buf StandardCharsets/UTF_8))))

;; concat is not a simple mono-call, as it permits a variable number of arguments so we can avoid intermediate alloc
(defmethod codegen-call :concat [{:keys [emitted-args]}]
  (when (< (count emitted-args) 2)
    (throw (IllegalArgumentException. "Arity error, concat requires at least two arguments")))
  (let [possible-types (into #{} (mapcat (comp types/flatten-union-types :return-type)) emitted-args)
        value-types (disj possible-types :null)
        _ (when (< 1 (count value-types)) (throw (IllegalArgumentException. "All arguments to concat must be of the same type.")))
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

(defmethod codegen-mono-call [:substring :utf8 :int :int :bool] [_]
  {:return-type :utf8
   :->call-code (fn [[x start length use-len]]
                  `(StringUtil/sqlUtf8Substring (resolve-utf8-buf ~x) ~start ~length ~use-len))})

(defmethod codegen-mono-call [:substring :varbinary :int :int :bool] [_]
  {:return-type :varbinary
   :->call-code (fn [[x start length use-len]]
                  `(StringUtil/sqlBinSubstring (resolve-buf ~x) ~start ~length ~use-len))})

(defmethod codegen-mono-call [:character-length :utf8 :utf8] [{:keys [args]}]
  (let [[_ unit] (map :literal args)]
    {:return-type :i32
     :->call-code (case unit
                    "CHARACTERS" #(do `(StringUtil/utf8Length (resolve-utf8-buf ~(first %))))
                    "OCTETS" #(do `(.remaining (resolve-utf8-buf ~(first %)))))}))

(defmethod codegen-mono-call [:octet-length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-utf8-buf ~@%)))})

(defmethod codegen-mono-call [:octet-length :varbinary] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-buf ~@%)))})

(defmethod codegen-mono-call [:position :utf8 :utf8 :utf8] [{:keys [args]}]
  (let [[_ _ unit] (map :literal args)]
    {:return-type :i32
     :->call-code (fn [[needle haystack]]
                    (case unit
                      "CHARACTERS" `(StringUtil/sqlUtf8Position (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack))
                      "OCTETS" `(StringUtil/SqlBinPosition (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack))))}))

(defmethod codegen-mono-call [:position :varbinary :varbinary] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]] `(StringUtil/SqlBinPosition (resolve-buf ~needle) (resolve-buf ~haystack)))})

(defmethod codegen-mono-call [:overlay :utf8 :utf8 :int :int] [_]
  {:return-type :utf8
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlUtf8Overlay (resolve-utf8-buf ~target) (resolve-utf8-buf ~replacement) ~from ~len))})

(defmethod codegen-mono-call [:overlay :varbinary :varbinary :int :int] [_]
  {:return-type :varbinary
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlBinOverlay (resolve-buf ~target) (resolve-buf ~replacement) ~from ~len))})

(defmethod codegen-mono-call [:default-overlay-length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(StringUtil/utf8Length (resolve-utf8-buf ~@%)))})

(defmethod codegen-mono-call [:default-overlay-length :varbinary] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-buf ~@%)))})

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
(defmethod codegen-mono-call [:= :interval :interval] [_]
  {:return-type :bool, :->call-code #(do `(boolean (interval-eq ~@%)))})

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

(defmethod codegen-mono-call [:single-field-interval :int :utf8 :int :int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit)
      (ensure-interval-fractional-precision-valid fractional-precision))

    (case unit
      "YEAR" {:return-type [:interval :year-month]
              :->call-code #(do `(PeriodDuration. (Period/ofYears ~(first %)) Duration/ZERO))}
      "MONTH" {:return-type [:interval :year-month]
               :->call-code #(do `(PeriodDuration. (Period/ofMonths ~(first %)) Duration/ZERO))}
      "DAY" {:return-type [:interval :day-time]
             :->call-code #(do `(PeriodDuration. (Period/ofDays ~(first %)) Duration/ZERO))}
      "HOUR" {:return-type [:interval :day-time]
              :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofHours ~(first %))))}
      "MINUTE" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes ~(first %))))}
      "SECOND" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofSeconds ~(first %))))})))

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

(defmethod codegen-mono-call [:single-field-interval :utf8 :utf8 :int :int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit)
      (ensure-interval-fractional-precision-valid fractional-precision))

    (case unit
      "YEAR" {:return-type [:interval :year-month]
              :->call-code #(do `(PeriodDuration. (Period/ofYears (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "MONTH" {:return-type [:interval :year-month]
               :->call-code #(do `(PeriodDuration. (Period/ofMonths (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "DAY" {:return-type [:interval :day-time]
             :->call-code #(do `(PeriodDuration. (Period/ofDays (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "HOUR" {:return-type [:interval :day-time]
              :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofHours (ensure-single-field-interval-int ~(first %)))))}
      "MINUTE" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes (ensure-single-field-interval-int ~(first %)))))}
      "SECOND" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (second-interval-fractional-duration ~(first %))))})))

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

(defmethod codegen-mono-call [:multi-field-interval :utf8 :utf8 :int :utf8 :int] [{:keys [args]}]
  (let [[_ unit1 precision unit2 fractional-precision] (map :literal args)]

    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit2)
      (ensure-interval-fractional-precision-valid fractional-precision))

    ;; TODO choose a more specific representation when possible
    {:return-type [:interval :month-day-nano]
     :->call-code (fn [[s & _]]
                    `(parse-multi-field-interval (resolve-string ~s) ~unit1 ~unit2))}))

(defmethod codegen-mono-call [:extract :utf8 :timestamp-tz] [{[{field :literal} _] :args}]
  {:return-type :i64
   :->call-code (fn [[_ ts-code]]
                  `(.get (.atOffset ^Instant (util/micros->instant ~ts-code) ZoneOffset/UTC)
                         ~(case field
                            "YEAR" `ChronoField/YEAR
                            "MONTH" `ChronoField/MONTH_OF_YEAR
                            "DAY" `ChronoField/DAY_OF_MONTH
                            "HOUR" `ChronoField/HOUR_OF_DAY
                            "MINUTE" `ChronoField/MINUTE_OF_HOUR)))})

(defmethod codegen-mono-call [:extract :utf8 :date] [{[{field :literal} _] :args}]
  ;; FIXME this assumes date-unit :day
  {:return-type :i32
   :->call-code (fn [[_ epoch-day-code]]
                  (case field
                    ;; we could inline the math here, but looking at sources, there is some nuance.
                    "YEAR" `(.getYear (LocalDate/ofEpochDay ~epoch-day-code))
                    "MONTH" `(.getMonthValue (LocalDate/ofEpochDay ~epoch-day-code))
                    "DAY" `(.getDayOfMonth (LocalDate/ofEpochDay ~epoch-day-code))
                    "HOUR" 0
                    "MINUTE" 0))})

(defmethod codegen-mono-call [:date-trunc :utf8 :timestamp-tz] [{[{field :literal} _] :args, [_ [_tstz _time-unit tz :as ts-type]] :arg-types}]
  ;; FIXME this assumes micros
  {:return-type ts-type
   :->call-code (fn [[_ x]]
                  `(util/instant->micros (-> (util/micros->instant ~x)
                                             ;; TODO only need to create the ZoneId once per batch
                                             ;; but getting calls to have batch-bindings isn't straightforward
                                             (ZonedDateTime/ofInstant (ZoneId/of ~tz))
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
                                             (.toInstant))))})

(defmethod codegen-mono-call [:date-trunc :utf8 :date] [{[{field :literal} _] :args, [_ [_date _date-unit :as date-type]] :arg-types}]
  ;; FIXME this assumes epoch-day
  {:return-type date-type
   :->call-code (fn [[_ epoch-day-code]]
                  (case field
                    "YEAR" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfYear 1) .toEpochDay)
                    "MONTH" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfMonth 1) .toEpochDay)
                    "DAY" epoch-day-code
                    "HOUR" epoch-day-code
                    "MINUTE" epoch-day-code))})

(def ^:dynamic ^java.time.Clock *clock* (Clock/systemDefaultZone))

(defn- bound-precision ^long [^long precision]
  (-> precision (max 0) (min 9)))

(def ^:private precision-timeunits
  [:second
   :milli :milli :milli
   :micro :micro :micro
   :nano :nano :nano])

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
        zone-id (.getZone *clock*)]
    {:return-type [:timestamp-tz (precision-timeunits precision) (str zone-id)]
     :->call-code (fn [_]
                    (-> `(long (let [inst# (.instant *clock*)]
                                 (+ (* ~(seconds-multiplier precision) (.getEpochSecond inst#))
                                    (quot (.getNano inst#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(def ^:private default-time-precision 6)

(defmethod codegen-mono-call [:current-timestamp] [_]
  (current-timestamp default-time-precision))

(defmethod codegen-mono-call [:current-timestamp :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-timestamp precision))

(defmethod codegen-mono-call [:current-date] [_]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Dates to be in UTC.
  ;; we then turn DateDays into LocalDates, which confuses things further.
  {:return-type [:date :day]
   :->call-code (fn [_]
                  `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                             (.toLocalDate)
                             (.toEpochDay))))})

(defn- current-time [^long precision]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Times to be in UTC.
  ;; we then turn times into LocalTimes, which confuses things further.
  (let [precision (bound-precision precision)]
    {:return-type [:time (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod codegen-mono-call [:current-time] [_]
  (current-time default-time-precision))

(defmethod codegen-mono-call [:current-time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-time precision))

(defn- local-timestamp [^long precision]
  (let [precision (bound-precision precision)]
    {:return-type [:timestamp-local (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (let [ldt# (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                                              (.toLocalDateTime))]
                                 (+ (* (.toEpochSecond ldt# ZoneOffset/UTC) ~(seconds-multiplier precision))
                                    (quot (.getNano ldt#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(defmethod codegen-mono-call [:local-timestamp] [_]
  (local-timestamp default-time-precision))

(defmethod codegen-mono-call [:local-timestamp :num] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-timestamp precision))

(defn- local-time [^long precision]
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)]
    {:return-type [:time time-unit]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod codegen-mono-call [:local-time] [_]
  (local-time default-time-precision))

(defmethod codegen-mono-call [:local-time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-time precision))

(defmethod codegen-mono-call [:abs :num] [{[numeric-type] :arg-types}]
  {:return-type numeric-type
   :->call-code #(do `(Math/abs ~@%))})

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
  (defmethod codegen-mono-call [math-op :num] [_]
    {:return-type :f64
     :->call-code #(do `(~math-method ~@%))}))

(defmethod codegen-mono-call [:cast :num] [{:keys [cast-type]}]
  {:return-type cast-type
   :->call-code #(do `(~(type->cast cast-type) ~@%))})

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti set-value-form
  (fn [value-type out-vec-sym idx-sym code]
    (types/col-type-head value-type))
  :default ::default
  :hierarchy #'types/col-type-hierarchy)

(defmethod set-value-form :null [_ _out-vec-sym _idx-sym _code])

(defmethod set-value-form :bool [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (if ~code 1 0)))

(defmethod set-value-form :int [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (long ~code)))

(defmethod set-value-form :float [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (double ~code)))

(defn- set-bytebuf-form [out-vec-sym idx-sym code]
  `(let [^ByteBuffer buf# ~code
         pos# (.position buf#)]
     (.setSafe ~out-vec-sym ~idx-sym buf#
               pos# (.remaining buf#))
     ;; setSafe mutates the buffer's position, so we reset it
     (.position buf# pos#)))

(defmethod set-value-form :utf8 [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form :varbinary [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form :timestamp-tz [_ out-vec-sym idx-sym code] `(.set ~out-vec-sym ~idx-sym ~code))
(defmethod set-value-form :timestamp-local [_ out-vec-sym idx-sym code] `(.set ~out-vec-sym ~idx-sym ~code))
(defmethod set-value-form :duration [_ out-vec-sym idx-sym code] `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form :date [_ out-vec-sym idx-sym code]
  ;; assuming we are writing to a date day vector, as that is the canonical representation
  ;; TODO not convinced we can assume this?
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form :time [_ out-vec-sym idx-sym code] `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form :interval [[_ interval-unit] out-vec-sym idx-sym code]
  (case interval-unit
    :year-month `(let [^PeriodDuration period-duration# ~code
                       period# (.getPeriod period-duration#)]
                   (.set ^IntervalYearVector ~out-vec-sym ~idx-sym (.toTotalMonths period#)))

    :day-time `(let [^PeriodDuration period-duration# ~code
                     period# (.getPeriod period-duration#)
                     duration# (.getDuration period-duration#)
                     ddays# (.toDaysPart duration#)
                     dsecs# (Math/subtractExact (.getSeconds duration#) (Math/multiplyExact ddays# (long 86400)))
                     dmillis# (.toMillisPart duration#)]
                 (.set ^IntervalDayVector ~out-vec-sym ~idx-sym (Math/addExact (.getDays period#) (int ddays#)) (Math/addExact (Math/multiplyExact (int dsecs#) (int 1000)) dmillis#)))

    :month-day-nano `(let [^PeriodDuration period-duration# ~code
                           period# (.getPeriod period-duration#)
                           duration# (.getDuration period-duration#)
                           ddays# (.toDaysPart duration#)
                           dsecs# (Math/subtractExact (.getSeconds duration#) (Math/multiplyExact ddays# (long 86400)))
                           dnanos# (.toNanosPart duration#)]
                       (.set ~out-vec-sym ~idx-sym (.toTotalMonths period#) (Math/addExact (.getDays period#) (int ddays#)) (Math/addExact (Math/multiplyExact dsecs# (long 1000000000)) (long dnanos#))))))

(def ^:private out-vec-sym (gensym 'out-vec))
(def ^:private out-writer-sym (gensym 'out-writer-sym))

(defmulti extension-type-literal-form class)

(defmethod extension-type-literal-form KeywordType [_] `KeywordType/INSTANCE)
(defmethod extension-type-literal-form UuidType [_] `UuidType/INSTANCE)

(defn- write-value-out-code [return-type]
  (let [out-field (types/col-type->field return-type)]
    (if (instance? ArrowType$Union (.getType out-field))
      (let [[_ inner-types] return-type
            ->writer-sym (->> inner-types
                              (into {} (map (juxt identity (fn [_] (gensym 'writer))))))]
        {:writer-bindings
         (->> (cons [out-writer-sym `(.asDenseUnion ~out-writer-sym)]
                    (for [[inner-type writer-sym] ->writer-sym]
                      [writer-sym `(.writerForType ~out-writer-sym ~inner-type)]))
              (apply concat))

         :write-value-out!
         (fn [return-type code]
           (let [writer-sym (->writer-sym return-type)
                 vec-type (-> (.getType (types/col-type->field return-type))
                              (types/arrow-type->vector-type))]
             `(do
                (.startValue ~writer-sym)
                ~(set-value-form return-type
                                 (-> `(.getVector ~writer-sym)
                                     (with-tag vec-type))
                                 `(.getPosition ~writer-sym)
                                 code)
                (.endValue ~writer-sym))))})

      {:write-value-out!
       (fn [value-type code]
         (when-not (= value-type :null)
           (let [vec-type (-> (.getType (types/col-type->field value-type))
                              (types/arrow-type->vector-type))]
             (set-value-form value-type
                             (-> `(.getVector ~out-writer-sym)
                                 (with-tag vec-type))
                             `(.getPosition ~out-writer-sym)
                             code))))})))

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
                        (walk/postwalk-expr (comp #(with-batch-bindings % opts) lit->param)))

              return-boxes (HashMap.)
              {:keys [return-type continue boxes]} (codegen-expr expr (-> opts (assoc :return-boxes return-boxes)))

              {:keys [writer-bindings write-value-out!]} (write-value-out-code return-type)]

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
                               ~(continue (fn [t c]
                                            (write-value-out! t c)))
                               (.endValue ~out-writer-sym))))

                        #_(doto clojure.pprint/pprint)
                        eval)

           :return-type return-type}))
      (memoize)
      wrap-zone-id-cache-buster))

(defn field->value-types [^Field field]
  ;; probably doesn't work with structs, not convinced about lists either.
  ;; can hopefully replace this with col-types
  (case (.name (Types/getMinorTypeForArrowType (.getType field)))
    "DENSEUNION"
    (mapv #(.getFieldType ^Field %) (.getChildren field))

    (.getFieldType field)))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-expr
  (fn [{:keys [op]} col-name {:keys [var-fields] :as opts}]
    op)
  :default ::default)

(defmethod emit-expr :struct [{:keys [entries]} col-name opts]
  (let [entries (vec (for [[k v] entries]
                       (emit-expr v (name k) opts)))
        ^Field return-field (apply types/->field (name col-name) types/struct-type false
                                   (map :return-field entries))]

    {:return-field return-field
     :eval-expr
     (fn [^IIndirectRelation in-rel, al params]
       (let [evald-entries (LinkedList.)]
         (try
           (doseq [{:keys [eval-expr]} entries]
             (.add evald-entries (eval-expr in-rel al params)))

           (let [row-count (.rowCount in-rel)
                 ^StructVector out-vec (.createVector return-field al)]
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
             (run! util/try-close evald-entries)))))}))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-call-expr
  (fn [{:keys [f] :as expr} out-vec]
    (keyword f)))

(defmethod emit-expr :vectorised-call [{:keys [args] :as expr} col-name opts]
  (let [emitted-args (mapv #(emit-expr % "arg" opts) args)
        {:keys [^Field return-field eval-call-expr]} (emit-call-expr (assoc expr :emitted-args emitted-args) col-name)]
    {:return-field return-field
     :eval-expr
     (fn [^IIndirectRelation in-rel al params]
       (let [evald-args (LinkedList.)]
         (try
           (doseq [{:keys [eval-expr]} emitted-args]
             (.add evald-args (eval-expr in-rel al params)))

           (let [out-vec (.createVector return-field al)]
             (try
               (eval-call-expr (vec evald-args) out-vec params)
               out-vec
               (catch Throwable t
                 (.close out-vec)
                 (throw t))))

           (finally
             (run! util/try-close evald-args)))))}))

(defmethod emit-call-expr :get-field [{[{^Field struct-field :return-field}] :emitted-args
                                       :keys [field]}
                                      col-name]
  {:return-field (letfn [(get-struct-child [^Field struct-field]
                           (first
                            (for [^Field child-field (.getChildren struct-field)
                                  :when (= (name field) (.getName child-field))]
                              child-field)))]
                   (if (instance? ArrowType$Union (.getType struct-field))
                     (types/->field (name col-name) types/dense-union-type false)
                     (or (get-struct-child struct-field)
                         (types/col-type->field col-name :null))))

   :eval-call-expr (fn [[^ValueVector in-vec] ^ValueVector out-vec _params]
                     (let [out-writer (vw/vec->writer out-vec)]
                       (.setValueCount out-vec (.getValueCount in-vec))

                       (let [col-rdr (-> (.structReader (iv/->direct-vec in-vec))
                                         (.readerForKey (name field)))
                             copier (.rowCopier col-rdr out-writer)]

                         (dotimes [idx (.getValueCount in-vec)]
                           (.startValue out-writer)
                           (.copyRow copier idx)
                           (.endValue out-writer)))))})

(defmethod emit-expr :variable [{:keys [variable]} col-name {:keys [var-fields]}]
  (let [^Field out-field (-> (or (get var-fields variable)
                                 (throw (IllegalArgumentException. (str "missing variable: " variable ", available " (pr-str (set (keys var-fields)))))))
                             (types/field-with-name (name col-name)))]
    {:return-field out-field
     :eval-expr
     (fn [^IIndirectRelation in-rel al _params]
       (let [in-vec (.vectorForName in-rel (name variable))
             out-vec (.createVector out-field al)]
         (.copyTo in-vec out-vec)
         (try
           out-vec
           (catch Throwable e
             (.close out-vec)
             (throw e)))))}))

(defmethod emit-expr :list [{:keys [elements]} col-name opts]
  (let [el-count (count elements)
        emitted-els (mapv #(emit-expr % "list-el" opts) elements)
        out-field (types/->field (name col-name) (ArrowType$FixedSizeList. el-count) false
                                 (types/->field "$data" types/dense-union-type false))]
    {:return-field out-field
     :eval-expr
     (fn [^IIndirectRelation in-rel al params]
       (let [els (LinkedList.)]
         (try
           (doseq [{:keys [eval-expr]} emitted-els]
             (.add els (eval-expr in-rel al params)))

           (let [out-vec (-> out-field
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
             (run! util/try-close els)))))}))

(defmethod emit-call-expr :nth-const-n [{:keys [^long n]} col-name]
  {:return-field (types/->field col-name types/dense-union-type false)

   :eval-call-expr
   (fn [[^ValueVector coll-vec] ^ValueVector out-vec _params]
     (let [list-rdr (.listReader (iv/->direct-vec coll-vec))
           coll-count (.getValueCount coll-vec)
           out-writer (vw/vec->writer out-vec)
           copier (.elementCopier list-rdr out-writer)]

       (.setValueCount out-vec coll-count)

       (dotimes [idx coll-count]
         (.startValue out-writer)
         (.copyElement copier idx n)
         (.endValue out-writer))))})

(defn- long-getter ^java.util.function.IntUnaryOperator [n-res]
  (condp = (class n-res)
    IntVector (reify IntUnaryOperator
                (applyAsInt [_ idx]
                  (.get ^IntVector n-res idx)))
    BigIntVector (reify IntUnaryOperator
                   (applyAsInt [_ idx]
                     (int (.get ^BigIntVector n-res idx))))))

(defmethod emit-call-expr :nth [_ col-name]
  (let [out-field (types/->field col-name types/dense-union-type false)]
    {:return-field out-field
     :eval-call-expr
     (fn [[^ValueVector coll-res ^ValueVector n-res] ^ValueVector out-vec _params]
       (let [list-rdr (.listReader (iv/->direct-vec coll-res))
             coll-count (.getValueCount coll-res)
             out-writer (.asDenseUnion (vw/vec->writer out-vec))
             copier (.elementCopier list-rdr out-writer)
             !null-writer (delay (.writerForType out-writer :null))]

         (.setValueCount out-vec coll-count)

         (let [->n (long-getter n-res)]
           (dotimes [idx coll-count]
             (.startValue out-writer)
             (if (or (not (.isPresent list-rdr idx))
                     (.isNull n-res idx))
               (doto ^IVectorWriter @!null-writer
                 (.startValue)
                 (.endValue))
               (.copyElement copier idx (.applyAsInt ->n idx)))
             (.endValue out-writer)))))}))

(defmethod emit-call-expr :trim-array [{[{^Field array-field :return-field}] :emitted-args}
                                       col-name]
  {:return-field (types/->field (name col-name) types/list-type true
                                (if (or (instance? ArrowType$List (.getType array-field))
                                        (instance? ArrowType$FixedSizeList (.getType array-field)))
                                  (first (.getChildren array-field))
                                  (types/->field "$data" types/dense-union-type false)))
   :eval-call-expr
   (fn [[^ValueVector array-res ^ValueVector n-res], ^ListVector out-vec, _params]
     (let [list-rdr (.listReader (iv/->direct-vec array-res))
           n-arrays (.getValueCount array-res)
           out-writer (.asList (vw/vec->writer out-vec))
           out-data-writer (.getDataWriter out-writer)
           copier (.elementCopier list-rdr out-data-writer)
           ->n (when-not (instance? NullVector n-res) (long-getter n-res))]
       (.setValueCount out-vec n-arrays)

       (dotimes [idx n-arrays]
         (.startValue out-writer)
         (if (or (not (.isPresent list-rdr idx))
                 (.isNull n-res idx))
           (.setNull out-vec idx)
           (let [element-count (.applyAsInt ->n idx)
                 end (.getElementEndIndex list-rdr idx)
                 start (.getElementStartIndex list-rdr idx)
                 nlen (- end start element-count)]
             ;; this exception is required by spec, but it would be sensible to just return an empty array
             (when (< nlen 0) (throw (IllegalArgumentException. "Data exception - array element error.")))
             (dotimes [n nlen]
               (.startValue out-data-writer)
               (.copyElement copier idx n)
               (.endValue out-data-writer))))
         (.endValue out-writer))))})

(def ^:private primitive-ops
  #{:variable :param :literal
    :call :let :local
    :if :if-some
    :metadata-field-present :metadata-vp-call})

(defn- emit-prim-expr [prim-expr col-name {:keys [var-fields var->col-type param-classes param-types] :as opts}]
  (let [prim-expr (->> prim-expr
                       (walk/prewalk-expr (fn [{:keys [op] :as expr}]
                                            (if (contains? primitive-ops op)
                                              expr
                                              {:op :variable
                                               :variable (gensym 'var)
                                               :idx idx-sym
                                               :expr expr}))))

        emitted-sub-exprs (for [{:keys [variable expr]} (->> (walk/expr-seq prim-expr)
                                                             (filter #(= :variable (:op %))))
                                :when expr]
                            (-> (emit-expr expr (name variable) opts)
                                (assoc :variable variable)))

        var->types (->> (concat (for [[variable field] var-fields]
                                  (MapEntry/create variable (field->value-types field)))
                                (for [{:keys [variable return-field]} emitted-sub-exprs]
                                  (MapEntry/create variable (field->value-types return-field))))
                        (into {}))

        var->col-type (into var->col-type
                            (for [{:keys [variable return-field]} emitted-sub-exprs]
                              (MapEntry/create variable (types/field->col-type return-field))))

        {:keys [expr-fn return-type]} (memo-generate-projection prim-expr
                                                                {:var->types var->types
                                                                 :var->col-type var->col-type
                                                                 :param-classes param-classes
                                                                 :param-types param-types})

        out-field (types/col-type->field col-name return-type)]

    ;; TODO what are we going to do about needing to close over locals?
    {:return-field out-field
     :eval-expr
     (fn [^IIndirectRelation in-rel al params]
       (let [evald-sub-exprs (HashMap.)]
         (try
           (doseq [{:keys [variable eval-expr]} emitted-sub-exprs]
             (.put evald-sub-exprs variable (eval-expr in-rel al params)))

           (let [out-vec (.createVector out-field al)]
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
             (run! util/try-close (vals evald-sub-exprs))))))}))

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

(defn ->param-types [params]
  (->> params
       (into {} (map (juxt key (comp types/value->col-type val))))))

(defn param-classes [params]
  (->> params (into {} (map (juxt key (comp class val))))))

(defn ->expression-projection-spec ^core2.operator.IProjectionSpec [col-name form {:keys [col-types] :as input-types}]
  (let [expr (form->expr form input-types)

        ;; HACK - this runs the analyser (we discard the emission) to get the widest possible out-type.
        ;; we assume that we don't need `:param-classes` nor accurate `var-fields`
        ;; (i.e. we lose the exact DUV type-id mapping by converting to col-types)
        ;; ideally we'd lose var-fields altogether.
        widest-out-type (-> (emit-expr expr col-name
                                       {:var->col-type col-types
                                        :var-fields (->> col-types
                                                         (into {} (map (juxt key
                                                                             (fn [[col-name col-type]]
                                                                               (types/col-type->field col-name col-type))))))})
                            :return-field
                            (types/field->col-type))]
    (reify IProjectionSpec
      (getColumnName [_] col-name)

      (getColumnType [_] widest-out-type)

      (project [_ allocator in-rel params]
        (let [var->col-type (->> (seq in-rel)
                                 (into {} (map (fn [^IIndirectVector iv]
                                                 [(symbol (.getName iv))
                                                  (types/field->col-type (.getField (.getVector iv)))]))))

              {:keys [eval-expr]} (emit-expr expr col-name {:param-classes (param-classes params)
                                                            :var-fields (->var-fields in-rel expr)
                                                            :var->col-type var->col-type})]
          (iv/->direct-vec (eval-expr in-rel allocator params)))))))

(defn ->expression-relation-selector ^core2.operator.IRelationSelector [form input-types]
  (let [projector (->expression-projection-spec "select" (list 'boolean form) input-types)]
    (reify IRelationSelector
      (select [_ al in-rel params]
        (with-open [selection (.project projector al in-rel params)]
          (let [^BitVector sel-vec (.getVector selection)
                res (IntStream/builder)]
            (dotimes [idx (.getValueCount selection)]
              (when (= 1 (.get sel-vec (.getIndex selection idx)))
                (.add res idx)))
            (.toArray (.build res))))))))
