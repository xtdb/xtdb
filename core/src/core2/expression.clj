(ns core2.expression
  (:require [clojure.string :as str]
            core2.operator.project
            core2.operator.select
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import clojure.lang.MapEntry
           core2.operator.project.ProjectionSpec
           [core2.operator.select IColumnSelector IRelationSelector]
           [core2.vector IIndirectRelation IIndirectVector]
           java.lang.reflect.Method
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Duration Instant ZoneOffset]
           [java.time.temporal ChronoField ChronoUnit]
           [java.util Date LinkedHashMap]
           org.apache.arrow.vector.BaseVariableWidthVector
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Duration ArrowType$FloatingPoint ArrowType$Int ArrowType$Timestamp ArrowType$Utf8]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn expand-variadics [{:keys [op] :as expr}]
  (letfn [(expand-l [{:keys [f args]}]
            (reduce (fn [acc arg]
                      {:op :call, :f f, :args [acc arg]})
                    args))

          (expand-r [{:keys [f args]}]
            (reduce (fn [acc arg]
                      {:op :call, :f f, :args [arg acc]})
                    (reverse args)))]

    (or (when (= :call op)
          (let [{:keys [f args]} expr]
            (when (> (count args) 2)
              (cond
                (contains? '#{+ - * /} f) (expand-l expr)
                (contains? '#{and or} f) (expand-r expr)
                (contains? '#{<= < = != > >=} f) (expand-r {:op :call
                                                            :f 'and
                                                            :args (for [[x y] (partition 2 1 args)]
                                                                    {:op :call
                                                                     :f f
                                                                     :args [x y]})})))))

        expr)))

(defn form->expr [form params]
  (cond
    (symbol? form) (if (contains? params form)
                     {:op :param, :param form}
                     {:op :variable, :variable form})

    (sequential? form) (let [[f & args] form]
                         (case f
                           'if (do
                                 (when-not (= 3 (count args))
                                   (throw (IllegalArgumentException. (str "'if' expects 3 args: " (pr-str form)))))

                                 (let [[pred then else] args]
                                   {:op :if,
                                    :pred (form->expr pred params),
                                    :then (form->expr then params),
                                    :else (form->expr else params)}))

                           (-> {:op :call, :f f, :args (mapv #(form->expr % params) args)}
                               expand-variadics)))

    :else {:op :literal, :literal form}))

(defmulti direct-child-exprs
  (fn [{:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod direct-child-exprs ::default [_] #{})
(defmethod direct-child-exprs :if [{:keys [pred then else]}] [pred then else])
(defmethod direct-child-exprs :call [{:keys [args]}] args)

(defmulti postwalk-expr
  (fn [f {:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod postwalk-expr ::default [f expr]
  (f expr))

(defmethod postwalk-expr :if [f {:keys [pred then else]}]
  (f {:op :if
      :pred (postwalk-expr f pred)
      :then (postwalk-expr f then)
      :else (postwalk-expr f else)}))

(defmethod postwalk-expr :call [f {expr-f :f, :keys [args]}]
  (f {:op :call
      :f expr-f
      :args (mapv #(postwalk-expr f %) args)}))

(defn lits->params [expr]
  (->> expr
       (postwalk-expr (fn [{:keys [op] :as expr}]
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

                          (let [child-exprs (direct-child-exprs expr)]
                            (-> expr
                                (vary-meta (fnil into {})
                                           {:literals (->> child-exprs
                                                           (into {} (mapcat (comp :literals meta))))
                                            :params (->> child-exprs
                                                         (into #{} (mapcat (comp :params meta))))}))))))))

(defn expr-seq [expr]
  (lazy-seq
   (cons expr (mapcat expr-seq (direct-child-exprs expr)))))

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(defn variables [expr]
  (->> (expr-seq expr)
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
   types/timestamp-milli-type 'long
   types/duration-milli-type 'long})

(def idx-sym (gensym "idx"))

(defmulti codegen-expr
  (fn [{:keys [op]} {:keys [var->type]}]
    op))

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
    (into {:code param
           :return-type return-type}
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
(defmethod get-value-form ArrowType$Duration [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Utf8 [_ vec-sym idx-sym] `(element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Binary [_ vec-sym idx-sym] `(element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form :default [_ vec-sym idx-sym] `(normalize-union-value (.getObject ~vec-sym ~idx-sym)))

(defn- arrow-type-literal-form [^ArrowType arrow-type]
  (let [minor-type-name (.name (Types/getMinorTypeForArrowType arrow-type))]
    (case minor-type-name
      "DURATION" (throw (UnsupportedOperationException.))
      ;; TODO there are other minor types that don't have a single corresponding ArrowType
      `(.getType ~(symbol (name 'org.apache.arrow.vector.types.Types$MinorType) minor-type-name)))))

(defmethod codegen-expr :variable [{:keys [variable]} {:keys [var->type]}]
  (let [arrow-type (or (get var->type variable)
                       (throw (IllegalArgumentException. (str "unknown variable: " variable))))
        vec-sym (gensym 'vec)]
    ;; TODO the `reader-for-type` call doesn't need to be per-elem
    {:code `(let [~(-> vec-sym
                       (with-tag (or (-> arrow-type types/arrow-type->vector-type)
                                     (throw (UnsupportedOperationException.)))))
                  (-> ~variable
                      (iv/reader-for-type ~(arrow-type-literal-form arrow-type))
                      (.getVector))
                  ~idx-sym (.getIndex ~variable ~idx-sym)]
              ~(get-value-form arrow-type vec-sym idx-sym))
     :return-type arrow-type}))

(defmethod codegen-expr :if [{:keys [pred then else]} _]
  (let [return-type (types/least-upper-bound [(:return-type then) (:return-type else)])
        cast (get type->cast return-type)]
    {:code (cond->> (list 'if (:code pred) (:code then) (:code else))
             cast (list cast))
     :return-type return-type}))

(defmulti codegen-call
  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (name f)) (map class arg-types))))
  :hierarchy #'types/arrow-type-hierarchy)

(defmethod codegen-expr :call [{:keys [args] :as expr} _]
  (codegen-call (-> expr
                    (assoc :emitted-args (mapv :code args)
                           :arg-types (mapv :return-type args)))))

(defmethod codegen-call [:= ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(== ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:= ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:= ::types/Number ::types/Number] [:= ::types/Object ::types/Object])

(defmethod codegen-call [:!= ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(not= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:!= ::types/Number ::types/Number] [:!= ::types/Object ::types/Object])

(defmethod codegen-call [:< ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(< ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:< ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(< ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:< ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(< ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:< ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(neg? (compare ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:< ArrowType$Binary ArrowType$Binary] [{:keys [emitted-args]}]
  {:code `(neg? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:< ArrowType$Utf8 ArrowType$Utf8] [{:keys [emitted-args]}]
  {:code `(neg? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:< ::types/Number ::types/Number] [:< ::types/Object ::types/Object])
(prefer-method codegen-call [:< ArrowType$Timestamp ArrowType$Timestamp] [:< ::types/Object ::types/Object])
(prefer-method codegen-call [:< ArrowType$Duration ArrowType$Duration] [:< ::types/Object ::types/Object])
(prefer-method codegen-call [:< ArrowType$Utf8 ArrowType$Utf8] [:< ::types/Object ::types/Object])

(defmethod codegen-call [:<= ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:<= ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:<= ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:<= ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:<= ArrowType$Binary ArrowType$Binary] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:<= ArrowType$Utf8 ArrowType$Utf8] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:<= ::types/Number ::types/Number] [:<= ::types/Object ::types/Object])
(prefer-method codegen-call [:<= ArrowType$Timestamp ArrowType$Timestamp] [:<= ::types/Object ::types/Object])
(prefer-method codegen-call [:<= ArrowType$Duration ArrowType$Duration] [:<= ::types/Object ::types/Object])
(prefer-method codegen-call [:<= ArrowType$Utf8 ArrowType$Utf8] [:<= ::types/Object ::types/Object])

(defmethod codegen-call [:> ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:> ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:> ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:> ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(pos? (compare ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:> ArrowType$Binary ArrowType$Binary] [{:keys [emitted-args]}]
  {:code `(pos? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:> ArrowType$Utf8 ArrowType$Utf8] [{:keys [emitted-args]}]
  {:code `(pos? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:> ::types/Number ::types/Number] [:> ::types/Object ::types/Object])
(prefer-method codegen-call [:> ArrowType$Timestamp ArrowType$Timestamp] [:> ::types/Object ::types/Object])
(prefer-method codegen-call [:> ArrowType$Duration ArrowType$Duration] [:> ::types/Object ::types/Object])
(prefer-method codegen-call [:> ArrowType$Utf8 ArrowType$Utf8] [:> ::types/Object ::types/Object])

(defmethod codegen-call [:>= ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:>= ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:>= ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:>= ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:>= ArrowType$Binary ArrowType$Binary] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:>= ArrowType$Utf8 ArrowType$Utf8] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type ArrowType$Bool/INSTANCE})

(prefer-method codegen-call [:>= ::types/Number ::types/Number] [:>= ::types/Object ::types/Object])
(prefer-method codegen-call [:>= ArrowType$Timestamp ArrowType$Timestamp] [:>= ::types/Object ::types/Object])
(prefer-method codegen-call [:>= ArrowType$Duration ArrowType$Duration] [:>= ::types/Object ::types/Object])
(prefer-method codegen-call [:>= ArrowType$Utf8 ArrowType$Utf8] [:>= ::types/Object ::types/Object])

(defmethod codegen-call [:and ArrowType$Bool ArrowType$Bool] [{:keys [emitted-args]}]
  {:code `(and ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:or ArrowType$Bool ArrowType$Bool] [{:keys [emitted-args]}]
  {:code `(or ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:not ArrowType$Bool] [{:keys [emitted-args]}]
  {:code `(not ~@emitted-args)
   :return-type ArrowType$Bool/INSTANCE})

(defn- with-math-integer-cast
  "java.lang.Math's functions only take int or long, so we introduce an up-cast if need be"
  [^ArrowType$Int arrow-type emitted-args]
  (let [arg-cast (if (= 64 (.getBitWidth arrow-type)) 'long 'int)]
    (map #(list arg-cast %) emitted-args)))

(defmethod codegen-call [:+ ArrowType$Int ArrowType$Int] [{:keys [emitted-args arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:code (list (type->cast return-type)
                 `(Math/addExact ~@(with-math-integer-cast return-type emitted-args)))
     :return-type return-type}))

(defmethod codegen-call [:+ ::types/Number ::types/Number] [{:keys [emitted-args arg-types]}]
  {:code `(+ ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:- ArrowType$Int ArrowType$Int] [{:keys [emitted-args arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:code (list (type->cast return-type)
                 `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args)))
     :return-type return-type}))

(defmethod codegen-call [:- ::types/Number ::types/Number] [{:keys [emitted-args arg-types]}]
  {:code `(- ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:- ArrowType$Int]
  [{:keys [emitted-args], [^ArrowType$Int x-type] :arg-types}]

  {:code (list (type->cast x-type)
               `(Math/negateExact ~@(with-math-integer-cast x-type emitted-args)))
   :return-type x-type})

(defmethod codegen-call [:- ::types/Number] [{:keys [emitted-args], [x-type] :arg-types}]
  {:code `(- ~@emitted-args)
   :return-type x-type})

(defmethod codegen-call [:* ArrowType$Int ArrowType$Int] [{:keys [emitted-args arg-types]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)
        arg-cast (if (= 64 (.getBitWidth return-type)) 'long 'int)]
    {:code (list (type->cast return-type)
                 `(Math/multiplyExact ~@(map #(list arg-cast %) emitted-args)))
     :return-type return-type}))

(defmethod codegen-call [:* ::types/Number ::types/Number] [{:keys [emitted-args arg-types]}]
  {:code `(* ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:% ::types/Number ::types/Number] [{:keys [emitted-args arg-types]}]
  {:code `(mod ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:/ ::types/Number ::types/Number] [{:keys [emitted-args arg-types]}]
  {:code `(/ ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:/ ArrowType$Int ArrowType$Int] [{:keys [emitted-args arg-types]}]
  {:code `(quot ~@emitted-args)
   :return-type (types/least-upper-bound arg-types)})

(defmethod codegen-call [:like ::types/Object ArrowType$Utf8] [{[{x :code} {:keys [literal]}] :args}]
  {:code `(boolean (re-find ~(re-pattern (str "^" (str/replace literal #"%" ".*") "$"))
                            (resolve-string ~x)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod codegen-call [:substr ::types/Object ArrowType$Int ArrowType$Int] [{[{x :code} {start :code} {length :code}] :args}]
  {:code `(ByteBuffer/wrap (.getBytes (subs (resolve-string ~x) (dec ~start) (+ (dec ~start) ~length))
                                      StandardCharsets/UTF_8))
   :return-type ArrowType$Utf8/INSTANCE})

(defmethod codegen-call [:extract ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} {x :code}] :args}]
  {:code `(.get (.atOffset (Instant/ofEpochMilli ~x) ZoneOffset/UTC)
                ~(case field
                   "YEAR" `ChronoField/YEAR
                   "MONTH" `ChronoField/MONTH_OF_YEAR
                   "DAY" `ChronoField/DAY_OF_MONTH
                   "HOUR" `ChronoField/HOUR_OF_DAY
                   "MINUTE" `ChronoField/MINUTE_OF_HOUR))
   :return-type types/bigint-type})

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} {x :code, date-type :return-type}] :args}]
  {:code `(.toEpochMilli (.truncatedTo (Instant/ofEpochMilli ~x)
                                       ~(case field
                                          ;; can't truncate instants to years/months
                                          "DAY" `ChronoUnit/DAYS
                                          "HOUR" `ChronoUnit/HOURS
                                          "MINUTE" `ChronoUnit/MINUTES
                                          "SECOND" `ChronoUnit/SECONDS)))
   :return-type date-type})

(def ^:private type->arrow-type
  {Double/TYPE types/float8-type
   Long/TYPE types/bigint-type
   Boolean/TYPE ArrowType$Bool/INSTANCE})

(doseq [^Method method (.getDeclaredMethods Math)
        :let [math-op (.getName method)
              param-types (map type->arrow-type (.getParameterTypes method))
              return-type (get type->arrow-type (.getReturnType method))]
        :when (and return-type (every? some? param-types))]
  (defmethod codegen-call (vec (cons (keyword math-op) (map class param-types))) [{:keys [emitted-args]}]
    {:code `(~(symbol "Math" math-op) ~@emitted-args)
     :return-type return-type}))

(defn normalize-union-value [v]
  (cond
    (instance? Date v)
    (.getTime ^Date v)
    (instance? Instant v)
    (.toEpochMilli ^Instant v)
    (instance? Duration v)
    (.toMillis ^Duration v)
    (string? v)
    (ByteBuffer/wrap (.getBytes ^String v StandardCharsets/UTF_8))
    (bytes? v)
    (ByteBuffer/wrap v)
    :else
    v))

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
                                            (let [arrow-type (types/value->arrow-type param-v)
                                                  normalized-expr-type (normalize-union-value param-v)
                                                  primitive-tag (get type->cast (types/value->arrow-type normalized-expr-type))]
                                              (MapEntry/create (cond-> param-k
                                                                 primitive-tag (with-tag primitive-tag))
                                                               arrow-type))))))

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

(defmethod set-value-form ArrowType$Bool [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (if ~code 1 0)))

(defmethod set-value-form ArrowType$Int [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (long ~code)))

(defmethod set-value-form ArrowType$FloatingPoint [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (double ~code)))

(defmethod set-value-form ArrowType$Utf8 [_ out-vec-sym idx-sym code]
  `(let [buf# ~code]
     (.setSafe ~out-vec-sym ~idx-sym buf#
               (.position buf#) (.remaining buf#))))

(defmethod set-value-form ArrowType$Binary [_ out-vec-sym idx-sym code]
  `(let [buf# ~code]
     (.setSafe ~out-vec-sym ~idx-sym buf#
               (.position buf#) (.remaining buf#))))

(defmethod set-value-form ArrowType$Timestamp [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Duration [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defn- generate-projection [expr var-types param-types]
  (let [codegen-opts {:var->type var-types, :param->type param-types}
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % codegen-opts) expr)
        variables (->> (keys var-types)
                       (map #(with-tag % IIndirectVector)))

        out-vec-sym (gensym 'out-vec)]

    {:code `(fn [~(-> out-vec-sym
                      (with-tag (-> return-type types/arrow-type->vector-type)))
                 [~@variables] [~@(keys param-types)] ^long row-count#]

              (dotimes [~idx-sym row-count#]
                ~(set-value-form return-type out-vec-sym idx-sym code)))
     :return-type return-type}))

(def ^:private memo-generate-projection (memoize generate-projection))
(def ^:private memo-eval (memoize eval))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form params)
                                                      (normalise-params params))]
    (reify ProjectionSpec
      (project [_ allocator in-rel]
        (let [in-cols (expression-in-cols in-rel expr)
              var-types (->> in-cols
                             (util/into-linked-map
                              (util/map-values (fn [_variable ^IIndirectVector read-col]
                                                 (assert read-col)
                                                 (->> (iv/col->arrow-types read-col)
                                                      (types/least-upper-bound))))))
              {:keys [code return-type]} (memo-generate-projection expr var-types param-types)
              expr-fn (memo-eval code)
              out-vec (.createVector (types/->field col-name return-type false) allocator)
              row-count (.rowCount in-rel)]
          (try
            (.setValueCount out-vec row-count)
            (expr-fn out-vec (vals in-cols) (vals emitted-params) row-count)
            (iv/->direct-vec out-vec)
            (catch Exception e
              (.close out-vec)
              (throw e))))))))

(defn- generate-selection [expr var-types param-types]
  (let [codegen-opts {:var->type var-types, :param->type param-types}
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % codegen-opts) expr)
        variables (->> (keys var-types)
                       (map #(with-tag % IIndirectVector)))]

    (assert (= ArrowType$Bool/INSTANCE return-type))
    `(fn [[~@variables] [~@(keys param-types)] ^long row-count#]
       (let [acc# (RoaringBitmap.)]
         (dotimes [~idx-sym row-count#]
           (try
             (when ~code
               (.add acc# ~idx-sym))
             (catch ClassCastException e#)))
         acc#))))

(def ^:private memo-generate-selection (memoize generate-selection))

(defn ->expression-relation-selector ^core2.operator.select.IRelationSelector [form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form params)
                                                      (normalise-params params))]
    (reify IRelationSelector
      (select [_ in]
        (if (pos? (.rowCount in))
          (let [in-cols (expression-in-cols in expr)
                var-types (->> in-cols
                               (util/into-linked-map
                                (util/map-values (fn [_variable ^IIndirectVector read-col]
                                                   (->> (iv/col->arrow-types read-col)
                                                        (types/least-upper-bound))))))
                expr-code (memo-generate-selection expr var-types param-types)
                expr-fn (memo-eval expr-code)]
            (expr-fn (vals in-cols) (vals emitted-params) (.rowCount in)))
          (RoaringBitmap.))))))

(defn ->expression-column-selector ^core2.operator.select.IColumnSelector [form params]
  (let [{:keys [expr param-types emitted-params]} (-> (form->expr form params)
                                                      (normalise-params params))
        vars (variables expr)
        _ (assert (= 1 (count vars)))
        variable (first vars)]
    (reify IColumnSelector
      (select [_ in-col]
        (if (pos? (.getValueCount in-col))
          (let [in-cols (doto (LinkedHashMap.)
                          (.put variable in-col))
                var-types (doto (LinkedHashMap.)
                            (.put variable (->> (iv/col->arrow-types in-col)
                                                (types/least-upper-bound))))
                expr-code (memo-generate-selection expr var-types param-types)
                expr-fn (memo-eval expr-code)]
            (expr-fn (vals in-cols) (vals emitted-params) (.getValueCount in-col)))
          (RoaringBitmap.))))))
