(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            core2.operator.project
            core2.operator.select
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           core2.operator.project.ProjectionSpec
           [core2.operator.select IColumnSelector IRelationSelector]
           core2.types.LegType
           [core2.vector IIndirectRelation IIndirectVector]
           [core2.vector.extensions KeywordType UuidType]
           java.lang.reflect.Method
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Duration Instant ZoneOffset]
           [java.time.temporal ChronoField ChronoUnit]
           [java.util Date LinkedHashMap]
           [org.apache.arrow.vector BaseVariableWidthVector DurationVector ValueVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types TimeUnit Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Duration ArrowType$FloatingPoint ArrowType$Int ArrowType$Null ArrowType$Timestamp ArrowType$Utf8 Field FieldType]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn form->expr [form {:keys [params locals] :as env}]
  (cond
    (or (true? form) (false? form) (nil? form))
    {:op :literal, :literal form}

    (symbol? form) (cond
                     (contains? locals form) {:op :local, :local form}
                     (contains? params form) {:op :param, :param form}
                     :else {:op :variable, :variable form})

    (sequential? form) (let [[f & args] form]
                         (case f
                           if (do
                                (when-not (= 3 (count args))
                                  (throw (IllegalArgumentException. (str "'if' expects 3 args: " (pr-str form)))))

                                (let [[pred then else] args]
                                  {:op :if,
                                   :pred (form->expr pred env),
                                   :then (form->expr then env),
                                   :else (form->expr else env)}))

                           let (do
                                 (when-not (= 2 (count args))
                                   (throw (IllegalArgumentException. (str "'let' expects 2 args - bindings + body" (pr-str form)))))

                                 (let [[bindings body] args]
                                   (when-not (or (nil? bindings) (sequential? bindings))
                                     (throw (IllegalArgumentException. (str "'let' expects a sequence of bindings: " (pr-str form)))))

                                   (if-let [[local expr-form & more-bindings] (seq bindings)]
                                     (do
                                       (when-not (symbol? local)
                                         (throw (IllegalArgumentException. (str "bindings in `let` should be symbols: " (pr-str local)))))
                                       {:op :let
                                        :local local
                                        :expr (form->expr expr-form env)
                                        :body (form->expr (list 'let more-bindings body) (update env :locals (fnil conj #{}) local))})

                                     (form->expr body env))))

                           {:op :call, :f f, :args (mapv #(form->expr % env) args)}))

    :else {:op :literal, :literal form}))

(defn lits->params [expr]
  (->> expr
       (walk/postwalk-expr (fn [{:keys [op] :as expr}]
                             (case op
                               :literal (let [{:keys [literal]} expr
                                              sym (gensym 'literal)]
                                          (-> {:op :param, :param sym, :literal literal}
                                              (vary-meta (fnil into {})
                                                         {:literals {sym literal}
                                                          :params #{sym}})))

                               :param (-> expr
                                          (vary-meta (fnil into {})
                                                     {:params #{(:param expr)}}))

                               (let [child-exprs (walk/direct-child-exprs expr)]
                                 (-> expr
                                     (vary-meta (fnil into {})
                                                {:literals (->> child-exprs
                                                                (into {} (mapcat (comp :literals meta))))
                                                 :params (->> child-exprs
                                                              (into #{} (mapcat (comp :params meta))))}))))))))

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(defn variables [expr]
  (->> (walk/expr-seq expr)
       (into [] (comp (filter (comp #(= :variable %) :op))
                      (map :variable)
                      (distinct)))))

(def type->cast
  {ArrowType$Bool/INSTANCE 'boolean
   (.getType Types$MinorType/TINYINT) 'byte
   (.getType Types$MinorType/SMALLINT) 'short
   (.getType Types$MinorType/INT) 'int
   types/bigint-type 'long
   types/float8-type 'double
   types/timestamp-micro-tz-type 'long
   types/duration-micro-type 'long})

(def idx-sym (gensym "idx"))

(defmulti codegen-expr
  "Returns a map containing
    * `:return-types` (set)
    * `:continue` (fn).
      Returned fn expects a function taking a single return-type and the emitted code for that return-type.
      May be called multiple times if there are multiple return types.
      Returned fn returns the emitted code for the expression (including the code supplied by the callback)."
  (fn [{:keys [op]} {:keys [var->types]}]
    op))

(defmethod codegen-expr :nil [_ _]
  {:return-types #{types/null-type}
   :continue (fn [f] (f types/null-type nil))})

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x))

(defmethod codegen-expr :param [{:keys [param] :as expr} {:keys [param->type]}]
  (let [return-type (or (get param->type param)
                        (throw (IllegalArgumentException. (str "parameter not provided: " param))))]
    (into {:return-types #{return-type}
           :continue (fn [f]
                       (f return-type param))}
          (select-keys expr #{:literal}))))

(defn compare-nio-buffers-unsigned ^long [^ByteBuffer x ^ByteBuffer y]
  (let [rem-x (.remaining x)
        rem-y (.remaining y)
        limit (min rem-x rem-y)
        char-limit (bit-shift-right limit 1)
        diff (.compareTo (.limit (.asCharBuffer x) char-limit)
                         (.limit (.asCharBuffer y) char-limit))]
    (if (zero? diff)
      (loop [n (bit-and-not limit 1)]
        (if (= n limit)
          (- rem-x rem-y)
          (let [x-byte (.get x n)
                y-byte (.get y n)]
            (if (= x-byte y-byte)
              (recur (inc n))
              (Byte/compareUnsigned x-byte y-byte)))))
      diff)))

(defn element->nio-buffer ^java.nio.ByteBuffer [^BaseVariableWidthVector vec ^long idx]
  (let [value-buffer (.getDataBuffer vec)
        offset-buffer (.getOffsetBuffer vec)
        offset-idx (* idx BaseVariableWidthVector/OFFSET_WIDTH)
        offset (.getInt offset-buffer offset-idx)
        end-offset (.getInt offset-buffer (+ offset-idx BaseVariableWidthVector/OFFSET_WIDTH))]
    (.nioBuffer value-buffer offset (- end-offset offset))))

(defmulti get-value-form
  (fn [arrow-type vec-sym idx-sym]
    (class arrow-type)))

(defmethod get-value-form ArrowType$Bool [_ vec-sym idx-sym] `(= 1 (.get ~vec-sym ~idx-sym)))
(defmethod get-value-form ArrowType$FloatingPoint [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Int [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Timestamp [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Duration [_ vec-sym idx-sym] `(DurationVector/get (.getDataBuffer ~vec-sym) ~idx-sym))
(defmethod get-value-form ArrowType$Utf8 [_ vec-sym idx-sym] `(element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Binary [_ vec-sym idx-sym] `(element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form :default [_ vec-sym idx-sym] `(normalize-union-value (.getObject ~vec-sym ~idx-sym)))

(defmethod codegen-expr :variable [{:keys [variable]} {:keys [var->types]}]
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
                     `(let [~var-idx-sym (.getIndex ~variable ~idx-sym)
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
         `(let [~var-idx-sym (.getIndex ~variable ~idx-sym)
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

(defmethod codegen-expr :call [expr opts]
  (if (not= (:op expr) :call)
    (codegen-expr expr opts)

    (let [{:keys [args]} expr
          emitted-args (mapv #(codegen-expr % opts) args)
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
          (wrap-mono-return)))))

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

  (doseq [arrow-type #{::types/Number ArrowType$Timestamp ArrowType$Duration}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [_]
      (mono-fn-call types/bool-type #(do `(~f-sym ~@%)))))

  (doseq [arrow-type #{ArrowType$Binary ArrowType$Utf8}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [_]
      (mono-fn-call types/bool-type #(cmp `(compare-nio-buffers-unsigned ~@%)))))

  (defmethod codegen-call [f-kw ::types/Object ::types/Object] [_]
    (mono-fn-call types/bool-type #(cmp `(compare ~@%)))))

(defmethod codegen-call [:= ::types/Number ::types/Number] [_]
  (mono-fn-call types/bool-type #(do `(== ~@%))))

(defmethod codegen-call [:= ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(= ~@%))))

(defmethod codegen-call [:!= ::types/Number ::types/Number] [_]
  (mono-fn-call types/bool-type #(do `(not (== ~@%)))))

(defmethod codegen-call [:!= ::types/Object ::types/Object] [_]
  (mono-fn-call types/bool-type #(do `(not= ~@%))))

(doseq [f-kw #{:= :!= :< :<= :> :>=}]
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

(defmethod codegen-call [:% ::types/Number ::types/Number] [{:keys [ arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(mod ~@%))))

(defmethod codegen-call [:/ ArrowType$Int ArrowType$Int] [{:keys [ arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(quot ~@%))))

(defmethod codegen-call [:/ ::types/Number ::types/Number] [{:keys [ arg-types]}]
  (mono-fn-call (types/least-upper-bound arg-types)
                #(do `(/ ~@%))))

(doseq [f #{:+ :- :* :/ :%}]
  (defmethod codegen-call [f ::types/Number ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ::types/Number] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defmethod codegen-call [:like ::types/Object ArrowType$Utf8] [{[_ {:keys [literal]}] :args}]
  {:return-types #{types/bool-type}
   :continue-call (fn [f [haystack-code]]
                    (f types/bool-type
                       `(boolean (re-find ~(re-pattern (str "^" (str/replace literal #"%" ".*") "$"))
                                          (resolve-string ~haystack-code)))))})

(defmethod codegen-call [:substr ::types/Object ArrowType$Int ArrowType$Int] [_]
  {:return-types #{ArrowType$Utf8/INSTANCE}
   :continue-call (fn [f [x start length]]
                    (f ArrowType$Utf8/INSTANCE
                       `(ByteBuffer/wrap (.getBytes (subs (resolve-string ~x) (dec ~start) (+ (dec ~start) ~length))
                                                    StandardCharsets/UTF_8))))})

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

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} _] :args, [_ date-type] :arg-types}]
  {:return-types #{date-type}
   :continue-call (fn [f [_ x]]
                    (f date-type
                       `(util/instant->micros (.truncatedTo ^Instant (util/micros->instant ~x)
                                                            ~(case field
                                                               ;; can't truncate instants to years/months
                                                               ;; ah, but you can truncate ZDTs, which is what these really are. TODO
                                                               "DAY" `ChronoUnit/DAYS
                                                               "HOUR" `ChronoUnit/HOURS
                                                               "MINUTE" `ChronoUnit/MINUTES
                                                               "SECOND" `ChronoUnit/SECONDS
                                                               "MILLISECOND" `ChronoUnit/MILLIS
                                                               "MICROSECOND" `ChronoUnit/MICROS)))))})

(def ^:private type->arrow-type
  {Double/TYPE types/float8-type
   Long/TYPE types/bigint-type
   Boolean/TYPE ArrowType$Bool/INSTANCE})

(doseq [^Method method (.getDeclaredMethods Math)
        :let [math-op (.getName method)
              param-types (map type->arrow-type (.getParameterTypes method))
              return-type (get type->arrow-type (.getReturnType method))]
        :when (and return-type (every? some? param-types))]
  (defmethod codegen-call (vec (cons (keyword math-op) (map class param-types))) [_]
    {:return-types #{return-type}
     :continue-call (fn [f emitted-args]
                      (f return-type
                         `(~(symbol "Math" math-op) ~@emitted-args)))}))

(defn normalize-union-value [v]
  (cond
    (instance? Date v) (Math/multiplyExact (.getTime ^Date v) 1000)
    (instance? Instant v) (util/instant->micros v)
    (instance? Duration v) (.toMillis ^Duration v)
    (string? v) (ByteBuffer/wrap (.getBytes ^String v StandardCharsets/UTF_8))
    (bytes? v) (ByteBuffer/wrap v)
    :else v))

(defn normalise-params [expr params]
  (let [expr (lits->params expr)
        {expr-params :params, lits :literals} (meta expr)
        params (->> expr-params
                    (util/into-linked-map
                     (map (fn [param-k]
                            (MapEntry/create param-k
                                             (val (or (find params param-k)
                                                      (find lits param-k))))))))]
    {:expr expr
     :params params
     :param-types (->> params
                       (util/into-linked-map
                        (util/map-entries (fn [param-k param-v]
                                            (let [leg-type (types/value->leg-type param-v)
                                                  normalized-expr-type (normalize-union-value param-v)
                                                  primitive-tag (get type->cast (.arrowType (types/value->leg-type normalized-expr-type)))]
                                              (MapEntry/create (cond-> param-k
                                                                 primitive-tag (with-tag primitive-tag))
                                                               (.arrowType leg-type)))))))

     :emitted-params (->> params
                          (util/into-linked-map
                           (util/map-values (fn [_param-k param-v]
                                              (normalize-union-value param-v)))))}))

(defn- expression-in-cols [^IIndirectRelation in-rel expr]
  (->> (variables expr)
       (util/into-linked-map
        (map (fn [variable]
               (MapEntry/create variable (.vectorForName in-rel (name variable))))))))

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

        ;; TODO there are other minor types that don't have a single corresponding ArrowType
        `(.getType ~(symbol (name 'org.apache.arrow.vector.types.Types$MinorType) minor-type-name))))))

(defn- return-types->field-type ^org.apache.arrow.vector.types.pojo.FieldType [return-types]
  (let [without-null (disj return-types types/null-type)]
    (if (>= (count without-null) 2)
      (FieldType. false types/dense-union-type nil)
      (FieldType. (contains? return-types types/null-type)
                  (first without-null)
                  nil))))

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

(def ^:private memo-generate-projection
  (-> (fn [expr var->types param-types]
        (let [variable-syms (->> (keys var->types)
                                 (mapv #(with-tag % IIndirectVector)))

              codegen-opts {:var->types var->types, :param->type param-types}
              {:keys [return-types continue]} (codegen-expr expr codegen-opts)
              ret-field-type (return-types->field-type return-types)

              {:keys [writer-bindings write-value-out!]} (write-value-out-code ret-field-type return-types)]

          {:expr-fn (eval
                     `(fn [~(-> out-vec-sym (with-tag ValueVector))
                           [~@variable-syms] [~@(keys param-types)] ^long row-count#]

                        (let [~out-writer-sym (vw/vec->writer ~out-vec-sym)
                              ~@writer-bindings]
                          (.setValueCount ~out-vec-sym row-count#)
                          (dotimes [~idx-sym row-count#]
                            (.startValue ~out-writer-sym)
                            ~(continue write-value-out!)
                            (.endValue ~out-writer-sym)))))

           :field-type ret-field-type}))
      (memoize)))

(defn field->value-types [^Field field]
  ;; potential duplication with LegType
  ;; probably doesn't work with structs, not convinced about lists either.
  (case (.name (Types/getMinorTypeForArrowType (.getType field)))
    "DENSEUNION"
    (mapv #(.getFieldType ^Field %) (.getChildren field))

    (.getFieldType field)))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [^String col-name form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form {:params params})
                                                      (macro/macroexpand-all)
                                                      (normalise-params params))]
    (reify ProjectionSpec
      (project [_ allocator in-rel]
        (let [in-cols (expression-in-cols in-rel expr)
              var-types (->> in-cols
                             (util/into-linked-map
                              (util/map-values (fn [variable ^IIndirectVector read-col]
                                                 (assert read-col (str "missing read-col: " variable))
                                                 (field->value-types (.getField (.getVector read-col)))))))
              {:keys [expr-fn ^FieldType field-type]} (memo-generate-projection expr var-types param-types)
              ^ValueVector out-vec (.createNewSingleVector field-type col-name allocator nil)]
          (try
            (expr-fn out-vec (vals in-cols) (vals emitted-params) (.rowCount in-rel))
            (iv/->direct-vec out-vec)
            (catch Exception e
              (.close out-vec)
              (throw e))))))))

(def ^:private memo-generate-selection
  (-> (fn [expr var->types param-types]
        (let [variable-syms (->> (keys var->types)
                                 (mapv #(with-tag % IIndirectVector)))

              codegen-opts {:var->types var->types, :param->type param-types}
              {:keys [return-types continue]} (codegen-expr expr codegen-opts)
              acc-sym (gensym 'acc)]

          (assert (= #{ArrowType$Bool/INSTANCE} return-types))

          (eval
           `(fn [[~@variable-syms] [~@(keys param-types)] ^long row-count#]
              (let [~acc-sym (RoaringBitmap.)]
                (dotimes [~idx-sym row-count#]
                  ~(continue (fn [_ code]
                               `(when ~code
                                  (.add ~acc-sym ~idx-sym)))))
                ~acc-sym)))))
      (memoize)))

(defn ->expression-relation-selector ^core2.operator.select.IRelationSelector [form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form {:params params})
                                                      (macro/macroexpand-all)
                                                      (normalise-params params))]
    (reify IRelationSelector
      (select [_ in-rel]
        (if (pos? (.rowCount in-rel))
          (let [in-cols (expression-in-cols in-rel expr)
                var-types (->> in-cols
                               (util/into-linked-map
                                (util/map-values (fn [_variable ^IIndirectVector read-col]
                                                   (field->value-types (.getField (.getVector read-col)))))))
                expr-fn (memo-generate-selection expr var-types param-types)]

            (expr-fn (vals in-cols) (vals emitted-params) (.rowCount in-rel)))
          (RoaringBitmap.))))))

(defn ->expression-column-selector ^core2.operator.select.IColumnSelector [form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form {:params params})
                                                      (macro/macroexpand-all)
                                                      (normalise-params params))
        vars (variables expr)
        _ (assert (= 1 (count vars)) (str "multiple vars in column selector: " (pr-str vars)))
        variable (first vars)]
    (reify IColumnSelector
      (select [_ in-col]
        (if (pos? (.getValueCount in-col))
          (let [in-cols (doto (LinkedHashMap.)
                          (.put variable in-col))
                var-types (doto (LinkedHashMap.)
                            (.put variable (field->value-types (.getField (.getVector in-col)))))
                expr-fn (memo-generate-selection expr var-types param-types)]
            (expr-fn (vals in-cols) (vals emitted-params) (.getValueCount in-col)))
          (RoaringBitmap.))))))
