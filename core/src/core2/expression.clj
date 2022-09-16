(ns core2.expression
  (:require [core2.error :as err]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            [core2.rewrite :refer [zmatch]]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector :as vec]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (clojure.lang Keyword MapEntry)
           (core2 StringUtil)
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.vector IIndirectRelation IIndirectVector IRowCopier)
           core2.vector.PolyValueBox
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneOffset ZonedDateTime)
           (java.util Arrays Date HashMap LinkedList)
           (java.util.function IntUnaryOperator)
           (java.util.regex Pattern)
           (java.util.stream IntStream)
           (org.apache.arrow.vector BigIntVector BitVector IntVector NullVector PeriodDuration ValueVector)
           (org.apache.arrow.vector.complex FixedSizeListVector ListVector StructVector)
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
                     (= 'core2/end-of-time form) {:op :literal, :literal util/end-of-time}
                     (contains? locals form) {:op :local, :local form}
                     (contains? param-types form) {:op :param, :param form, :param-type (get param-types form)}
                     (contains? col-types form) {:op :variable, :variable form, :var-type (get col-types form)}
                     :else (throw (err/illegal-arg :core2.expression/unknown-symbol
                                                   {::err/message (format "Unknown symbol: '%s'" form)
                                                    :symbol form})))

    (map? form) (do
                  (when-not (every? keyword? (keys form))
                    (throw (err/illegal-arg :core2.expression/parse-error
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
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message (str "'if' expects 3 args: " (pr-str form))
                             :form form})))

  (let [[pred then else] args]
    {:op :if,
     :pred (form->expr pred env),
     :then (form->expr then env),
     :else (form->expr else env)}))

(defmethod parse-list-form 'let [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message (str "'let' expects 2 args - bindings + body"
                                                 (pr-str form))
                             :form form})))

  (let [[bindings body] args]
    (when-not (or (nil? bindings) (sequential? bindings))
      (throw (err/illegal-arg :core2.expression/parse-error
                              {::err/message (str "'let' expects a sequence of bindings: "
                                                   (pr-str form))
                               :form form})))

    (if-let [[local expr-form & more-bindings] (seq bindings)]
      (do
        (when-not (symbol? local)
          (throw (err/illegal-arg :core2.expression/parse-error
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
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message (str "'.' expects 2 args: " (pr-str form))
                             :form form})))

  (let [[struct field] args]
    (when-not (or (symbol? field) (keyword? field))
      (throw (err/illegal-arg :core2.expression/arity-error
                              {::err/message (str "'.' expects symbol or keyword fields: " (pr-str form))
                               :form form})))

    {:op :vectorised-call
     :f :get-field
     :args [(form->expr struct env)]
     :field (symbol field)}))

(defmethod parse-list-form '.. [[_ & args :as form] env]
  (let [[struct & fields] args]
    (when-not (seq fields)
      (throw (err/illegal-arg :core2.expression/arity-error
                              {::err/message (str "'..' expects at least 2 args: " (pr-str form))
                               :form form})))
    (when-not (every? #(or (symbol? %) (keyword? %)) fields)
      (throw (err/illegal-arg :core2.expression/parse-error
                              {::err/message (str "'..' expects symbol or keyword fields: " (pr-str form))
                               :form form})))
    (reduce (fn [struct-expr field]
              {:op :vectorised-call
               :f :get-field
               :args [struct-expr]
               :field (symbol field)})
            (form->expr struct env)
            (rest args))))

(defmethod parse-list-form 'nth [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message (str "'nth' expects 2 args: " (pr-str form))
                             :form form})))

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

(defmethod parse-list-form 'cardinality [[_ & args :as form] env]
  (when-not (= 1 (count args))
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message (str "'cardinality' expects 1 arg: " (pr-str form))
                             :form form})))

  {:op :vectorised-call
   :f :cardinality
   :args [(form->expr (first args) env)]})

(defmethod parse-list-form 'trim-array [[_ arr n] env]
  {:op :vectorised-call
   :f :trim-array
   :args [(form->expr arr env)
          (form->expr n env)]})

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
         :duration 'long}
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

(defmethod read-value-code :null [_ & _args] nil)
(defmethod write-value-code :null [_ & args] `(.writeNull ~@args))

(doseq [[k sym] '{:bool Boolean, :i8 Byte, :i16 Short, :i32 Int, :i64 Long, :f32 Float, :f64 Double
                  :date Int, :time-local Long, :timestamp-tz Long, :timestamp-local Long, :duration Long, :interval Object
                  :utf8 Buffer, :varbinary Buffer}]
  (defmethod read-value-code k [_ & args] `(~(symbol (str ".read" sym)) ~@args))
  (defmethod write-value-code k [_ & args] `(~(symbol (str ".write" sym)) ~@args)))

(defmethod read-value-code :extension-type [[_ _ underlying-type _] & args]
  (apply read-value-code underlying-type args))

(defmethod write-value-code :extension-type [[_ _ underlying-type _] & args]
  (apply write-value-code underlying-type args))

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

(defn prepare-expr [expr]
  (->> expr
       (macro/macroexpand-all)
       (walk/postwalk-expr lit->param)))

(defmethod codegen-expr :param [{:keys [param param-type] :as expr} {:keys [param-types param-classes]}]
  (let [param-type (get param-types param param-type)
        param-class (get expr :param-class (get param-classes param))]
    (into {:return-type param-type
           :batch-bindings [[param
                             (emit-value param-class
                                         (get expr :literal
                                              (cond-> `(get ~params-sym '~param)
                                                param-class (with-tag param-class))))]]
           :continue (fn [f]
                       (f param-type param))}
          (select-keys expr #{:literal}))))

(defn- wrap-boxed-poly-return [{:keys [return-type continue] :as emitted-expr} _]
  (zmatch return-type
    [:union inner-types]
    (let [type-ids (->> inner-types
                        (into {} (map-indexed (fn [idx val-type]
                                                (MapEntry/create val-type (byte idx))))))
          box-sym (gensym 'box)]
      (-> emitted-expr
          (update :batch-bindings (fnil conj []) [box-sym `(PolyValueBox.)])
          (assoc :continue (fn [f]
                             `(do
                                ~(continue (fn [return-type code]
                                             (write-value-code return-type box-sym (get type-ids return-type) code)))

                                (case (.getTypeId ~box-sym)
                                  ~@(->> (for [[ret-type type-id] type-ids]
                                           [type-id (f ret-type (read-value-code ret-type box-sym))])
                                         (apply concat))))))))

    emitted-expr))

(defmethod codegen-expr :variable [{:keys [variable rel idx extract-vec-from-rel?],
                                    :or {rel rel-sym, idx idx-sym, extract-vec-from-rel? true}}
                                   {:keys [var->col-type extract-vecs-from-rel?]
                                    :or {extract-vecs-from-rel? true}}]
  ;; NOTE we now get the widest var-type in the expr itself, but don't use it here (yet? at all?)
  (let [col-type (or (get var->col-type variable)
                     (throw (AssertionError. (str "unknown variable: " variable))))]

    ;; NOTE: when used from metadata exprs, incoming vectors might not exist

    (zmatch col-type
      [:union inner-types]
      (let [ordered-col-types (vec inner-types)]
        {:return-type col-type
         :batch-bindings (if (and extract-vecs-from-rel? extract-vec-from-rel?)
                           [[variable `(.polyReader (.vectorForName ~rel ~(name variable))
                                                    ~ordered-col-types)]]
                           [[variable `(some-> ~variable (.polyReader ~ordered-col-types))]])
         :continue (fn [f]
                     `(case (.read ~variable ~idx)
                        ~@(->> ordered-col-types
                               (sequence
                                (comp (map-indexed (fn [type-id col-type]
                                                     [type-id (f col-type (read-value-code col-type variable))]))
                                      cat)))))})

      {:return-type col-type
       :batch-bindings (if (and extract-vecs-from-rel? extract-vec-from-rel?)
                         [[variable `(.monoReader (.vectorForName ~rel ~(name variable)))]]
                         [[variable `(some-> ~variable (.monoReader))]])
       :continue (fn [f]
                   (f col-type (read-value-code col-type variable idx)))})))

(defmethod codegen-expr :if [{:keys [pred then else] :as expr} opts]
  (let [{p-ret :return-type, p-cont :continue, :as emitted-p} (codegen-expr pred opts)
        {t-ret :return-type, t-cont :continue, :as emitted-t} (codegen-expr then opts)
        {e-ret :return-type, e-cont :continue, :as emitted-e} (codegen-expr else opts)
        return-type (types/merge-col-types t-ret e-ret)]
    (when-not (= :bool p-ret)
      (throw (err/illegal-arg :core2.expression/type-error
                              {::err/message (str "pred expression doesn't return boolean "
                                                   (pr-str p-ret))
                               :expr expr, :return-type return-type})))

    (-> {:return-type return-type
         :children [emitted-p emitted-t emitted-e]
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

          (let [{e-ret :return-type, e-cont :continue, :as emitted-else} (codegen-expr else opts)]
            {:return-type (apply types/merge-col-types e-ret then-rets)
             :children (cons emitted-else children)
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
    (vec (cons (keyword (namespace f) (name f))
               (map types/col-type-head arg-types))))

  :hierarchy #'types/col-type-hierarchy)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-call
  (fn [{:keys [f] :as expr}]
    (keyword f))
  :default ::default)

(def ^:private shortcut-null-args?
  (complement (comp #{:true? :false? :nil? :boolean :null-eq
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
     :batch-bindings (->> (vals emitted-calls) (sequence (mapcat :batch-bindings)))
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
                       (codegen-expr arg opts))]
    (-> (codegen-call (-> expr (assoc :emitted-args emitted-args)))
        (assoc :children emitted-args)
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
(defmethod codegen-mono-call [:null-eq :any :any] [call]
  (codegen-mono-call (assoc call :f :=)))

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
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/addExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:+ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(+ ~@%))})

(defmethod codegen-mono-call [:- :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:- :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
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
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(Math/multiplyExact ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-mono-call [:* :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(* ~@%))})

(defmethod codegen-mono-call [:mod :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(mod ~@%))})

(defmethod codegen-mono-call [:/ :int :int] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(quot ~@%))})

(defmethod codegen-mono-call [:/ :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(/ ~@%))})

;; TODO extend min/max to variable width
(defmethod codegen-mono-call [:max :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(Math/max ~@%))})

(defmethod codegen-mono-call [:min :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
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
  (when-not (trim-char-conforms? trim-char) (throw (err/runtime-err :core2.expression/data-exception
                                                                    {::err/message "Data Exception - trim error."})))
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
(defmethod codegen-call :concat [{:keys [emitted-args] :as expr}]
  (when (< (count emitted-args) 2)
    (throw (err/illegal-arg :core2.expression/arity-error
                            {::err/message "Arity error, concat requires at least two arguments"
                             :expr (dissoc expr :emitted-args :arg-types)})))
  (let [possible-types (into #{} (mapcat (comp types/flatten-union-types :return-type)) emitted-args)
        value-types (disj possible-types :null)
        _ (when (< 1 (count value-types)) (throw (err/illegal-arg :core2.expression/type-error
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

(defmulti codegen-cast
  (fn [{:keys [source-type target-type]}]
    [(types/col-type-head source-type) (types/col-type-head target-type)])
  :default ::default
  :hierarchy #'types/col-type-hierarchy)

(defmethod codegen-cast ::default [{:keys [source-type target-type]}]
  (throw (err/illegal-arg :core2.expression/cast-error
                          {::err/message (format "Unsupported cast: '%s' -> '%s'"
                                                 (pr-str source-type) (pr-str target-type))
                           :source-type source-type
                           :target-type target-type})))

(defmethod codegen-cast [:num :num] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(~(type->cast target-type) ~@%))})

(defmethod codegen-cast [:num :utf8] [_]
  {:return-type :utf8, :->call-code #(do `(resolve-utf8-buf (str ~@%)))})

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

(defmethod codegen-mono-call [:cast :any] [{[source-type] :arg-types, :keys [target-type]}]
  (codegen-cast {:source-type source-type, :target-type target-type}))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-expr
  (fn [{:keys [op]} col-name opts]
    op)
  :default ::default)

(defmethod emit-expr :struct [{:keys [entries]} col-name opts]
  (let [entries (vec (for [[k v] entries]
                       (MapEntry/create k (emit-expr v (name k) opts))))
        return-type [:struct (->> entries
                                  (into {} (map (juxt (comp symbol key)
                                                      (comp :return-type val)))))]
        return-field (types/col-type->field col-name return-type)]

    {:return-type return-type
     :eval-expr
     (fn [^IIndirectRelation in-rel, al params]
       (let [evald-entries (LinkedList.)]
         (try
           (doseq [{:keys [eval-expr]} (vals entries)]
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
  (fn [{:keys [f] :as expr}]
    (keyword f)))

(defmethod emit-expr :vectorised-call [{:keys [args] :as expr} col-name opts]
  (let [emitted-args (mapv #(emit-expr % "arg" opts) args)
        {:keys [return-type eval-call-expr]} (emit-call-expr (assoc expr :emitted-args emitted-args))
        return-field (types/col-type->field col-name return-type)]
    {:return-type return-type
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

(defmethod emit-call-expr :get-field [{[{struct-type :return-type}] :emitted-args, :keys [field]}]
  {:return-type (letfn [(->inner-types [col-type]
                          (zmatch col-type
                            [:union inner-types] (mapcat ->inner-types inner-types)
                            [:struct col-types] (some-> (get col-types field) vector)
                            []))]
                  (->> (->inner-types struct-type)
                       (apply types/merge-col-types :null)))

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

(defmethod emit-expr :variable [{:keys [variable]} col-name {:keys [var->col-type]}]
  (let [out-type (or (get var->col-type variable)
                     (throw (err/illegal-arg :core2.expression/parse-error
                                             (let [available-vars (set (keys var->col-type))]
                                               {::err/message (str "missing variable: " variable ", available " (pr-str available-vars))
                                                :missing-variable :variable
                                                :available-variables available-vars}))))]
    {:return-type out-type
     :eval-expr
     (fn [^IIndirectRelation in-rel al _params]
       (let [in-vec (.vectorForName in-rel (name variable))
             out-vec (.createVector (-> (.getField (.getVector in-vec))
                                        (types/field-with-name col-name))
                                    al)]
         (try
           (.copyTo in-vec out-vec)
           out-vec
           (catch Throwable e
             (.close out-vec)
             (throw e)))))}))

(defmethod emit-expr :list [{:keys [elements]} col-name opts]
  (let [el-count (count elements)
        emitted-els (mapv #(emit-expr % "list-el" opts) elements)
        out-type [:fixed-size-list el-count (->> emitted-els (map :return-type) (apply types/merge-col-types))]
        out-field (types/col-type->field col-name out-type)]
    {:return-type out-type
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

(defn- ->list-el-type [list-type {:keys [nullable?]}]
  (letfn [(->inner-types [col-type]
            (zmatch col-type
              [:union inner-types] (mapcat ->inner-types inner-types)
              [:fixed-size-list _ inner-type] [inner-type]
              [:list inner-type] [inner-type]))]
    (-> (->inner-types list-type)
        (cond->> nullable? (cons :null))
        (->> (apply types/merge-col-types)))))

(defmethod emit-call-expr :nth-const-n [{:keys [^long n], [{:keys [return-type]}] :emitted-args}]
  {:return-type (->list-el-type return-type {:nullable? true})

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

(defmethod emit-call-expr :nth [{[{:keys [return-type]}] :emitted-args}]
  {:return-type (->list-el-type return-type {:nullable? true})
   :eval-call-expr
   (fn [[^ValueVector coll-res ^ValueVector n-res] ^ValueVector out-vec _params]
     (let [list-rdr (.listReader (iv/->direct-vec coll-res))
           coll-count (.getValueCount coll-res)
           out-writer (vw/vec->writer out-vec)
           copier (.elementCopier list-rdr out-writer)]

       (.setValueCount out-vec coll-count)

       (let [->n (long-getter n-res)]
         (dotimes [idx coll-count]
           (.startValue out-writer)
           (if (or (not (.isPresent list-rdr idx))
                   (.isNull n-res idx))
             (doto out-writer
               (.startValue)
               (.endValue))
             (.copyElement copier idx (.applyAsInt ->n idx)))
           (.endValue out-writer)))))})

(defmethod emit-call-expr :cardinality [_]
  {:return-type [:union #{:i32 :null}]
   :eval-call-expr
   (fn [[^ValueVector coll-res] ^IntVector out-vec _params]
     (let [list-rdr (.listReader (iv/->direct-vec coll-res))
           coll-count (.getValueCount coll-res)]

       (.allocateNew out-vec coll-count)

       (dotimes [idx coll-count]
         (when (.isPresent list-rdr idx)
           (.set out-vec idx (- (.getElementEndIndex list-rdr idx)
                                (.getElementStartIndex list-rdr idx)))))

       (.setValueCount out-vec coll-count)))})

(defmethod emit-call-expr :trim-array [{[{array-type :return-type}] :emitted-args}]
  {:return-type [:list (->list-el-type array-type {:nullable? false})]
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
             (when (< nlen 0) (throw (err/runtime-err :core2.expression/array-element-error
                                                      {::err/message "Data exception - array element error."
                                                       :nlen nlen})))
             (dotimes [n nlen]
               (.startValue out-data-writer)
               (.copyElement copier idx n)
               (.endValue out-data-writer))))
         (.endValue out-writer))))})

(def ^:private out-vec-sym (gensym 'out-vec))
(def ^:private out-writer-sym (gensym 'out-writer-sym))

(defn batch-bindings [emitted-expr]
  (letfn [(child-seq [{:keys [children] :as expr}]
            (lazy-seq
             (cons expr (mapcat child-seq children))))]
    (->> (for [{:keys [batch-bindings]} (child-seq emitted-expr)
               batch-binding batch-bindings]
           batch-binding)
         (sequence (comp (distinct) cat)))))

(defn- write-value-out-code [return-type]
  (zmatch return-type
    [:union inner-types]
    (let [ordered-col-types (vec inner-types)
          type-ids (->> ordered-col-types
                        (into {} (map-indexed (fn [idx val-type]
                                                (MapEntry/create val-type (byte idx))))))]
      {:writer-bindings [out-writer-sym `(vec/->poly-writer ~out-vec-sym ~ordered-col-types)]

       :write-value-out! (fn [value-type code]
                           (write-value-code value-type out-writer-sym (get type-ids value-type) code))})

    {:writer-bindings [out-writer-sym `(vec/->mono-writer ~out-vec-sym)]
     :write-value-out! (fn [value-type code]
                         (write-value-code value-type out-writer-sym code))}))

(defn- wrap-zone-id-cache-buster [f]
  (fn [expr opts]
    (f expr (assoc opts :zone-id (.getZone *clock*)))))

(def ^:private memo-generate-projection
  "NOTE: we macroexpand inside the memoize on the assumption that
   everything outside yields the same result on the pre-expanded expr - this
   assumption wouldn't hold if macroexpansion created new variable exprs, for example.
   macroexpansion is non-deterministic (gensym), so busts the memo cache."
  (-> (fn [expr opts]
        ;; TODO should lit->param be outside the memoize, s.t. we don't have a cache entry for each literal value?
        (let [expr (prepare-expr expr)
              {:keys [return-type continue] :as emitted-expr} (codegen-expr expr opts)

              {:keys [writer-bindings write-value-out!]} (write-value-out-code return-type)]

          {:expr-fn (delay
                      (-> `(fn [~(-> rel-sym (with-tag IIndirectRelation))
                                ~params-sym
                                ~(-> out-vec-sym (with-tag ValueVector))]
                             (let [~@(batch-bindings emitted-expr)
                                   ~@writer-bindings
                                   row-count# (.rowCount ~rel-sym)]
                               (dotimes [~idx-sym row-count#]
                                 ~(continue (fn [t c]
                                              (write-value-out! t c))))))

                          #_(doto clojure.pprint/pprint)
                          eval))

           :return-type return-type}))
      (util/lru-memoize)
      wrap-zone-id-cache-buster))

(def ^:private primitive-ops
  #{:variable :param :literal
    :call :let :local
    :if :if-some
    :metadata-field-present :metadata-vp-call})

(defn- emit-prim-expr [prim-expr col-name {:keys [var->col-type param-classes param-types] :as opts}]
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

        var->col-type (into var->col-type
                            (for [{:keys [variable return-type]} emitted-sub-exprs]
                              (MapEntry/create variable return-type)))

        {:keys [expr-fn return-type]} (memo-generate-projection prim-expr
                                                                {:var->col-type var->col-type
                                                                 :param-classes param-classes
                                                                 :param-types param-types})
        out-field (types/col-type->field col-name return-type)]

    ;; TODO what are we going to do about needing to close over locals?
    {:return-type return-type
     :eval-expr
     (fn [^IIndirectRelation in-rel al params]
       (let [evald-sub-exprs (HashMap.)]
         (try
           (doseq [{:keys [variable eval-expr]} emitted-sub-exprs]
             (.put evald-sub-exprs variable (eval-expr in-rel al params)))

           (let [out-vec (.createVector out-field al)]
             (try
               (let [row-count (.rowCount in-rel)
                     in-rel (-> (concat in-rel (for [[variable sub-expr-vec] evald-sub-exprs]
                                                 (-> (iv/->direct-vec sub-expr-vec)
                                                     (.withName (name variable)))))
                                (iv/->indirect-rel row-count))]

                 (.setInitialCapacity out-vec row-count)
                 (.allocateNew out-vec)

                 (@expr-fn in-rel params out-vec)

                 (.setValueCount out-vec row-count))

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

(defn ->param-types [params]
  (->> params
       (into {} (map (juxt key (comp types/value->col-type val))))))

(defn param-classes [params]
  (->> params (into {} (map (juxt key (comp class val))))))

(defn ->expression-projection-spec ^core2.operator.IProjectionSpec [col-name form {:keys [col-types param-types] :as input-types}]
  (let [expr (form->expr form input-types)

        ;; HACK - this runs the analyser (we discard the emission) to get the widest possible out-type.
        ;; we assume that we don't need `:param-classes`
        widest-out-type (-> (emit-expr expr col-name
                                       {:param-types param-types
                                        :var->col-type col-types})
                            :return-type)]
    (reify IProjectionSpec
      (getColumnName [_] col-name)

      (getColumnType [_] widest-out-type)

      (project [_ allocator in-rel params]
        (let [var->col-type (->> (seq in-rel)
                                 (into {} (map (fn [^IIndirectVector iv]
                                                 [(symbol (.getName iv))
                                                  (types/field->col-type (.getField (.getVector iv)))]))))

              {:keys [eval-expr]} (emit-expr expr col-name {:param-types (->param-types params)
                                                            :param-classes (param-classes params)
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
