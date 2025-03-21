(ns xtdb.expression
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.expression.macro :as macro]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang IPersistentMap Keyword MapEntry)
           (java.lang NumberFormatException)
           [java.net URI]
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Duration Instant InstantSource LocalDate LocalDateTime LocalTime OffsetDateTime ZoneId ZoneOffset ZonedDateTime)
           (java.util Arrays Date List Map UUID)
           (java.util.regex Pattern)
           (java.util.stream IntStream)
           (org.apache.arrow.vector PeriodDuration ValueVector)
           (org.apache.commons.codec.binary Hex)
           (xtdb.arrow ListValueReader RelationReader ValueReader VectorPosition VectorReader)
           xtdb.arrow.ValueBox
           (xtdb.operator ProjectionSpec SelectionSpec)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.util StringUtil)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti parse-list-form
  (fn [[f & args] env]
    f)
  :default ::default)

(def postgres-server-version "16")

(defn xtdb-server-version []
  ;; TODO this won't work if the user has XT in-process in their own git repo.
  (or (System/getenv "XTDB_VERSION")

      (util/implementation-version)

      (when-let [git-sha (System/getenv "GIT_SHA")]
        (subs git-sha 0 7))

      (let [{:keys [out ^long exit]} (sh/sh "git" "rev-parse" "--short" "HEAD")]
        (when (zero? exit)
          (str/trim out)))

      "unknown"))

(defn form->expr [form {:keys [col-types param-types locals] :as env}]
  (cond
    (symbol? form) (cond
                     (= 'xtdb/end-of-time form) {:op :literal, :literal time/end-of-time}
                     (= 'xtdb/postgres-server-version form) {:op :literal, :literal (str "PostgreSQL " postgres-server-version)}
                     (= 'xtdb/xtdb-server-version form) {:op :literal, :literal (str "XTDB @ " (xtdb-server-version))}
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

    (vector? form) {:op :list, :elements (mapv #(form->expr % env) form)}
    (set? form) {:op :set, :elements (mapv #(form->expr % env) form)}

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

(defmethod parse-list-form 'cast [[_ expr target-type cast-opts] env]
  {:op :call
   :f :cast
   :args [(form->expr expr env)]
   :target-type target-type
   :cast-opts cast-opts})

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

(def type->unchecked-cast
  (comp {:bool 'boolean
         :i8 'unchecked-byte
         :i16 'unchecked-short
         :i32 'unchecked-int
         :i64 'unchecked-long
         :f32 'unchecked-float
         :f64 'unchecked-double
         :timestamp-tz 'unchecked-long
         :duration 'unchecked-long
         :decimal 'bigdec}
        types/col-type-head))

(def idx-sym (gensym 'idx))
(def rel-sym (gensym 'rel))
(def schema-sym (gensym 'schema))
(def args-sym (gensym 'args))

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

(doseq [int-type [:int :uint]]
  (defmethod codegen-cast [int-type :bool] [_]
    {:return-type :bool, :->call-code #(do `(not (zero? ~@%)))}))

(defmethod codegen-cast [:num :num] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(~(type->cast target-type) ~@%))})

(defmethod codegen-cast [:num :utf8] [_]
  {:return-type :utf8, :->call-code #(do `(resolve-utf8-buf (str ~@%)))})

(defmethod codegen-cast [:utf8 :uuid] [_]
  {:return-type :uuid, :->call-code #(do `(util/uuid->byte-buffer
                                           (UUID/fromString (resolve-string ~@%))))})

(defmethod codegen-cast [:utf8 :varbinary] [_]
  {:return-type :varbinary , :->call-code #(do `(ByteBuffer/wrap (Hex/decodeHex (resolve-string ~@%))))})

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
    :time-local Long, :timestamp-tz Long, :timestamp-local Long, :duration Long, :tstz-range Object
    :utf8 Bytes, :varbinary Bytes, :keyword Bytes, :uuid Bytes, :uri Bytes

    :list Object, :set Object, :struct Object :transit Object})

(defmethod read-value-code :null [_ & _args] nil)

(defmethod write-value-code :null [_ w & args]
  ;; This evals the args to perform any side effects
  `(do ~@args (.writeNull ~w)))

(doseq [k [:bool :i8 :i16 :i32 :i64 :f32 :f64
           :timestamp-tz :timestamp-local :time-local :duration :tstz-range
           :utf8 :varbinary :uuid :uri :keyword :transit]
        :let [rw-fn (col-type->rw-fn k)]]
  (defmethod read-value-code k [_ & args] `(~(symbol (str ".read" rw-fn)) ~@args))
  (defmethod write-value-code k [_ & args] `(~(symbol (str ".write" rw-fn)) ~@args)))

(defmethod read-value-code :date [[_ date-unit] & args]
  (case date-unit
    :day `(.readInt ~@args)
    :milli `(.readLong ~@args)))

(defmethod write-value-code :date [[_ date-unit] & args]
  (case date-unit
    :day `(.writeInt ~@args)
    :milli `(.writeLong ~@args)))

(defmethod write-value-code :regoid [_ & args] `(.writeInt ~@args))

(doseq [[k tag] {:interval PeriodDuration :decimal BigDecimal,
                 :list ListValueReader, :set ListValueReader, :struct Map}]
  (defmethod read-value-code k [_ & args]
    (-> `(.readObject ~@args) (with-tag tag)))

  (defmethod write-value-code k [_ & args] `(.writeObject ~@args)))

(defn- continue-read-union [f inner-types reader-sym args]
  (let [without-null (disj inner-types :null)]
    (if (empty? without-null)
      (f :null nil)

      (let [nn-code (if (= 1 (count without-null))
                      (let [nn-type (first (seq without-null))]
                        (f nn-type (read-value-code nn-type reader-sym)))

                      `(case (.getLeg ~reader-sym ~@args)
                         ;; https://ask.clojure.org/index.php/13407/method-large-compiling-expression-unfortunate-clause-hashes
                         :wont-ever-be-this (throw (IllegalStateException.))

                         ~@(->> inner-types
                                (mapcat (fn [col-type]
                                          ;; HACK
                                          [(types/col-type->leg col-type)
                                           (f col-type (read-value-code col-type reader-sym))])))))]
        (if (contains? inner-types :null)
          `(if (.isNull ~reader-sym ~@args)
             ~(f :null nil)
             ~nn-code)

          nn-code)))))

(defn- continue-read [f col-type reader-sym & args]
  (zmatch col-type
    [:union inner-types] (continue-read-union f inner-types reader-sym args)

    (f col-type (apply read-value-code col-type reader-sym args))))

(def ^:dynamic ^java.time.Instant *snapshot-time* nil)
(def ^:dynamic ^java.time.InstantSource *clock* (InstantSource/system))
(defn current-time ^java.time.Instant [] (.instant *clock*))
(def ^:dynamic ^java.time.ZoneId *default-tz* (ZoneId/systemDefault))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-value
  (fn [val-class code] val-class)
  :default ::default)

(defmethod emit-value ::default [_ code] code)
(defmethod emit-value Date [_ code] `(Math/multiplyExact (.getTime ~code) 1000))
(defmethod emit-value Instant [_ code] `(time/instant->micros ~code))
(defmethod emit-value ZonedDateTime [_ code] `(time/instant->micros (time/->instant ~code)))
(defmethod emit-value OffsetDateTime [_ code] `(time/instant->micros (time/->instant ~code)))
(defmethod emit-value LocalDateTime [_ code] `(time/instant->micros (.toInstant ~code ZoneOffset/UTC)))
(defmethod emit-value LocalTime [_ code] `(.toNanoOfDay ~code))
(defmethod emit-value Duration [_ code] `(quot (.toNanos ~code) 1000))
(defmethod emit-value UUID [_ code] `(util/uuid->byte-buffer ~code))
(defmethod emit-value URI [_ code] `(util/uri->byte-buffer ~code))

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

(defn prepare-expr [expr]
  (->> expr
       (macro/macroexpand-all)))

(defn- wrap-boxed-poly-return [{:keys [return-type continue] :as emitted-expr} _]
  (zmatch return-type
    [:union inner-types]
    (let [box-sym (gensym 'box)

          types (->> inner-types
                     (into {} (map (fn [val-type]
                                     (let [leg (types/col-type->leg val-type)]
                                       (MapEntry/create val-type {:leg leg
                                                                  :sym (symbol (str box-sym "--" leg))}))))))]
      (-> emitted-expr
          (update :batch-bindings (fnil into []) (into [[box-sym `(ValueBox.)]]
                                                       (map (fn [{:keys [leg sym]}]
                                                              [sym `(.legWriter ~box-sym ~leg)]))
                                                       (vals types)))
          (assoc :continue (fn [f]
                             `(do
                                ~(continue (fn [return-type code]
                                             (write-value-code return-type (:sym (get types return-type)) code)))

                                (case (.getLeg ~box-sym)
                                  ;; https://ask.clojure.org/index.php/13407/method-large-compiling-expression-unfortunate-clause-hashes
                                  :wont-ever-be-this (throw (IllegalStateException.))
                                  ~@(->> (for [[ret-type {:keys [leg]}] types]
                                           [leg (f ret-type (read-value-code ret-type box-sym))])
                                         (apply concat))))))))

    emitted-expr))

(defmethod codegen-expr :variable [{:keys [variable rel idx extract-vec-from-rel?],
                                    :or {rel rel-sym, idx idx-sym, extract-vec-from-rel? true}}
                                   {:keys [var->col-type extract-vecs-from-rel?]
                                    :or {extract-vecs-from-rel? true}}]
  ;; NOTE we now get the widest var-type in the expr itself, but don't use it here (yet? at all?)
  (let [vpos-sym (gensym 'vpos)
        col-type (or (get var->col-type variable)
                     (throw (AssertionError. (str "unknown variable: " variable))))
        var-rdr-sym (gensym (util/symbol->normal-form-symbol variable))]

    ;; NOTE: when used from metadata exprs, incoming vectors might not exist
    {:return-type col-type
     :batch-bindings [[vpos-sym `(VectorPosition/build)]
                      (if (and extract-vecs-from-rel? extract-vec-from-rel?)
                        [var-rdr-sym `(.valueReader (.get ~rel ~(str variable)) ~vpos-sym)]
                        [var-rdr-sym `(some-> ~variable (.valueReader ~vpos-sym))])]
     :continue (fn [f]
                 `(do
                    (.setPosition ~vpos-sym ~idx)
                    ~(continue-read f col-type var-rdr-sym)))}))

(defmethod codegen-expr :param [{:keys [param]} {:keys [param-types]}]
  (codegen-expr {:op :variable, :variable param, :rel args-sym, :idx 0}
                {:var->col-type {param (get param-types param)}}))

(defmethod codegen-expr :if [{:keys [pred then else]} opts]
  (let [{p-cont :continue, :as emitted-p} (codegen-expr {:op :call, :f :boolean, :args [pred]} opts)
        {t-ret :return-type, t-cont :continue, :as emitted-t} (codegen-expr then opts)
        {e-ret :return-type, e-cont :continue, :as emitted-e} (codegen-expr else opts)
        return-type (types/merge-col-types t-ret e-ret)]
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

(def normalise-fn-name
  (-> (fn [f]
        (let [f (keyword (namespace f) (name f))]
          (case f
            (:- :/) f
            (util/kw->normal-form-kw f))))
      memoize))

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
    (vec (cons (normalise-fn-name f)
               (map types/col-type-head arg-types))))

  :hierarchy #'types/col-type-hierarchy
  :default ::default)

(defmethod codegen-call ::default [{:keys [f arg-types]}]
  (throw (err/illegal-arg :xtdb.expression/function-type-mismatch
                          {::err/message (apply str (name (normalise-fn-name f)) " not applicable to types "
                                                (let [types (map (comp name types/col-type-head) arg-types)]
                                                  (concat (interpose ", " (butlast types))
                                                          (when (seq (butlast types)) [" and "])
                                                          [(last types)])))})))

(def ^:private shortcut-null-args?
  (complement (comp #{:is_true :is_false :is_null :true? :false? :nil? :boolean
                      :null_eq :compare_nulls_first :compare_nulls_last
                      :period :str :_patch}
                    normalise-fn-name)))

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

        emitted-calls (->> (for [arg-types all-arg-types]
                             (MapEntry/create arg-types
                                              (if (or (not shortcut-null?)
                                                      (every? #(not= :null %) arg-types))
                                                (codegen-call (assoc expr
                                                                     :args emitted-args
                                                                     :arg-types arg-types))
                                                {:return-type :null, :->call-code (constantly nil)})))
                           (into {}))

        return-type (->> (vals emitted-calls)
                         (into #{} (map :return-type))
                         (apply types/merge-col-types))]

    {:return-type return-type
     :batch-bindings (->> (vals emitted-calls) (into [] (mapcat :batch-bindings)))
     :continue (fn continue-call-expr [handle-emitted-expr]
                 (let [build-args-then-call
                       (reduce (fn step [build-next-arg {continue-this-arg :continue}]
                                 ;; step: emit this arg, and pass it through to the inner build-fn
                                 (fn continue-building-args [arg-types emitted-args]
                                   (continue-this-arg (fn [arg-type emitted-arg]
                                                        (if (and shortcut-null? (= :null arg-type))
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

(defmethod codegen-call [:is_true :any] [expr] (codegen-call (assoc expr :f :true?)))
(defmethod codegen-call [:is_false :any] [expr] (codegen-call (assoc expr :f :false?)))
(defmethod codegen-call [:is_null :any] [expr] (codegen-call (assoc expr :f :nil?)))

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

(defmethod codegen-call [:bit_not :int] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code #(do `(bit-not ~@%))})

(defmethod codegen-call [:bit_and :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(bit-and ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:bit_or :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(bit-or ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:bit_xor :int :int] [{:keys [arg-types]}]
  (let [return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :->call-code (fn [emitted-args]
                    (list (type->cast return-type)
                          `(bit-xor ~@(with-math-integer-cast return-type emitted-args))))}))

(defmethod codegen-call [:bit_shift_left :int :int] [{[left-arg-type _] :arg-types}]
  {:return-type left-arg-type
   :->call-code (fn [emitted-args]
                  (list (type->unchecked-cast left-arg-type)
                    `(bit-shift-left ~@(with-math-integer-cast left-arg-type emitted-args))))})

(defmethod codegen-call [:bit_shift_right :int :int] [{[left-arg-type _] :arg-types}]
  {:return-type left-arg-type
   :->call-code (fn [emitted-args]
                  `(bit-shift-right ~@(with-math-integer-cast left-arg-type emitted-args)))})

(defmethod codegen-call [:mod :num :num] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(mod ~@%))})

(defn throw-div-0 []
  (throw (err/runtime-err ::division-by-zero
                          {::err/message "data exception - division by zero"})))

(defmethod codegen-call [:/ :int :int] [{:keys [arg-types]}]
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(quot ~@%))})

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

(defmethod codegen-call [:_iid :i64] [_]
  {:return-type [:fixed-size-binary 16]
   :->call-code (fn [[id]]
                  `(resolve-buf (util/->iid ~id)))})

(defmethod codegen-call [:_iid :utf8] [_]
  {:return-type [:fixed-size-binary 16]
   :->call-code (fn [[id]]
                  `(resolve-buf (util/->iid (buf->str ~id))))})

(defmethod codegen-call [:_iid :uuid] [_]
    {:return-type [:fixed-size-binary 16]
     :->call-code (fn [[id]]
                    `(resolve-buf (util/->iid (util/byte-buffer->uuid ~id))))})

(defmethod codegen-call [:_iid :keyword] [_]
  {:return-type [:fixed-size-binary 16]
   :->call-code (fn [[id]]
                  `(resolve-buf (util/->iid (keyword (buf->str ~id)))))})

(defmethod codegen-call [:like :varbinary :varbinary] [{[_ {:keys [literal]}] :args}]
  {:return-type :bool
   :->call-code (fn [[haystack-code needle-code]]
                  `(boolean (re-find ~(if literal
                                        (like->regex (binary->hex-like-pattern (resolve-bytes literal)))
                                        `(like->regex (binary->hex-like-pattern (buf->bytes ~needle-code))))
                                     (Hex/encodeHexString (buf->bytes ~haystack-code)))))})

(defmethod codegen-call [:like_regex :utf8 :utf8 :utf8] [{:keys [args]}]
  (let [[_ re-literal flags] (map :literal args)

        flag-map
        {\s [Pattern/DOTALL]
         \i [Pattern/CASE_INSENSITIVE, Pattern/UNICODE_CASE]
         \m [Pattern/MULTILINE]}

        flag-int (int (reduce bit-or 0 (mapcat flag-map flags)))]

    {:return-type :bool
     :->call-code (fn [[haystack-code needle-code]]
                    `(boolean (re-find ~(if re-literal
                                          `(Pattern/compile ~re-literal ~flag-int)
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

(defmethod codegen-call [:trim_leading :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s trim-char]]
                  `(-> (sql-trim-leading (resolve-string ~s) (resolve-string ~trim-char))
                       (.getBytes StandardCharsets/UTF_8)
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim_trailing :utf8 :utf8] [_]
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

(defmethod codegen-call [:trim_leading :varbinary :varbinary] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  `(-> (resolve-bytes ~s)
                       (binary-trim-leading (first (resolve-bytes ~trim-octet)))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim_trailing :varbinary :varbinary] [_]
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

(defmethod codegen-call [:trim_leading :varbinary :num] [_]
  {:return-type :varbinary
   :->call-code (fn [[s trim-octet]]
                  ;; should we throw an explicit error if no good cast to byte is possible?
                  `(-> (resolve-bytes ~s)
                       (binary-trim-leading (byte ~trim-octet))
                       ByteBuffer/wrap))})

(defmethod codegen-call [:trim_trailing :varbinary :num] [_]
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
                  `(str->buf (.toUpperCase (resolve-string ~code))))})

(defmethod codegen-call [:lower :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[code]]
                  `(str->buf (.toLowerCase (resolve-string ~code))))})

(defmethod codegen-call [:namespace :keyword] [_]
  {:return-type [:union #{:null :utf8}]
   :continue-call (fn [f [kw]]
                    (let [res (symbol 'ns)]
                      `(if-let [~res (-> (buf->str ~kw)
                                         (symbol) (namespace))]
                         ~(f :utf8 `(str->buf ~res))
                         ~(f :null nil))))})

(defmethod codegen-call [:local_name :keyword] [_]
  {:return-type :utf8
   :->call-code (fn [[kw]]
                  `(-> (buf->str ~kw) (symbol) (name) str->buf))})

(defmethod codegen-call [:keyword :utf8] [_]
  {:return-type :keyword
   :->call-code first})

(defmethod codegen-cast [:keyword :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[code]]
                  `(-> (str ":" (buf->str ~code))
                       str->buf))})

(defmethod codegen-cast [:utf8 :keyword] [_]
  {:return-type :keyword
   :->call-code (fn [[code]]
                  `(let [kw# (buf->str ~code)]
                     (if (= \: (first kw#))
                       (str->buf (subs kw# 1))
                       (str->buf kw#))))})

(defmethod codegen-call [:str :utf8] [_]
  {:return-type :utf8
   :->call-code first})

(defmethod codegen-call [:str :null] [_]
  {:return-type :utf8
   :->call-code (constantly `(str->buf ""))})

(defmethod codegen-call [:str :any] [{[src-type] :arg-types, :as expr}]
  (codegen-cast (assoc expr
                       :source-type src-type
                       :target-type :utf8)))

(defmethod codegen-call [:random] [_]
  {:return-type :f64
   :->call-code (fn [_] '(Math/random))})

;; SQL Session Variable functions.
;; We currently return hard-coded values for these functions, as we do not currently have any of these concepts.
;; note from the users PoV the search path doesn't include implicitly included schemas such as pg_catalog
(def explicit-search-path ["public"])
(def implicit-search-path ["pg_catalog"])
(def search-path (concat implicit-search-path explicit-search-path))

(defmethod codegen-call [:current_user] [_]
  {:return-type :utf8 :->call-code (fn [_] `(ByteBuffer/wrap (.getBytes "xtdb" StandardCharsets/UTF_8)))})

(defmethod codegen-call [:current_database] [_]
  {:return-type :utf8 :->call-code (fn [_] `(ByteBuffer/wrap (.getBytes "xtdb" StandardCharsets/UTF_8)))})

(defmethod codegen-call [:current_schema] [_]
  {:return-type :utf8 :->call-code (fn [_] `(ByteBuffer/wrap (.getBytes "public" StandardCharsets/UTF_8)))})

(defn current-schemas [include-implicit?]
  (let [schemas (if include-implicit? search-path explicit-search-path)
        schema-entry-box (ValueBox.)]
    (reify ListValueReader
      (size [_] (count schemas))
      (nth [_ idx]
        (.writeBytes
         schema-entry-box
         (ByteBuffer/wrap
          (.getBytes ^String (nth schemas idx) StandardCharsets/UTF_8)))
        schema-entry-box))))

(defmethod codegen-call [:current_schemas :bool] [_]
    {:return-type [:list :utf8]
     :->call-code (fn [[include-implicit?]]
                    `(current-schemas ~include-implicit?))})

(defn parse-version [version-str]
  (let [[^long major, ^long minor, ^long patch] (->> (re-find #"^(\d+)\.(\d+)\.(\d+)" version-str)
                                                     rest
                                                     (map parse-long))]
    (+ (* major 1000000)
       (* minor 1000)
       patch)))

(defn current-setting [setting-name]
  (if (= setting-name "server_version_num")
    (parse-version (xtdb-server-version))
    (throw (UnsupportedOperationException. "Setting not supported"))))

(defmethod codegen-call [:current_setting :utf8] [_]
  {:return-type :i64
   :->call-code (fn [[setting-name]]
                  `(current-setting (resolve-string ~setting-name)))})

(defn sleep [^long duration unit]
  (Thread/sleep (long (* duration (quot (types/ts-units-per-second unit) 1000)))))

(defmethod codegen-call [:sleep :duration] [{[{[_duration unit] :return-type} _] :args}]
  {:return-type :null
   :->call-code (fn [[lit]]
                  `(sleep ~lit ~unit))})

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

(defmethod codegen-call [:character_length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(StringUtil/utf8Length (resolve-utf8-buf ~(first %))))})

(defmethod codegen-call [:octet_length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-utf8-buf ~@%)))})

(defmethod codegen-call [:octet_length :varbinary] [_]
  {:return-type :i32
   :->call-code #(do `(.remaining (resolve-buf ~@%)))})

(defmethod codegen-call [:position :utf8 :utf8] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/sqlUtf8Position (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))})

(defmethod codegen-call [:octet_position :utf8 :utf8] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/SqlBinPosition (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))})

(defmethod codegen-call [:octet_position :varbinary :varbinary] [_]
  {:return-type :i32
   :->call-code (fn [[needle haystack]]
                  `(StringUtil/SqlBinPosition (resolve-buf ~needle) (resolve-buf ~haystack)))})

(defmethod codegen-call [:overlay :utf8 :utf8 :int :int] [_]
  {:return-type :utf8
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlUtf8Overlay (resolve-utf8-buf ~target) (resolve-utf8-buf ~replacement) ~from ~len))})

(defmethod codegen-call [:replace :utf8 :utf8 :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[s target replacement]]
                  `(-> (.replace (resolve-string ~s) (resolve-string ~target) (resolve-string ~replacement))
                       (resolve-utf8-buf)))})

(defmethod codegen-call [:overlay :varbinary :varbinary :int :int] [_]
  {:return-type :varbinary
   :->call-code (fn [[target replacement from len]]
                  `(StringUtil/sqlBinOverlay (resolve-buf ~target) (resolve-buf ~replacement) ~from ~len))})

(defmethod codegen-call [:default_overlay_length :utf8] [_]
  {:return-type :i32
   :->call-code #(do `(StringUtil/utf8Length (resolve-utf8-buf ~@%)))})

(defmethod codegen-call [:default_overlay_length :varbinary] [_]
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

(defn str->buf ^ByteBuffer [^String s]
  (-> (.getBytes s StandardCharsets/UTF_8) (ByteBuffer/wrap)))

(doseq [[col-type parse-sym] [[:i8 `Byte/parseByte]
                              [:i16 `Short/parseShort]
                              [:i32 `Integer/parseInt]
                              [:i64 `Long/parseLong]
                              [:f32 `Float/parseFloat]
                              [:f64 `Double/parseDouble]]]
  (defmethod codegen-cast [:utf8 col-type] [_]
    {:return-type col-type, :->call-code #(do `(~parse-sym (buf->str ~@%)))}))

(defmethod codegen-call [:cast :any] [{[source-type] :arg-types, :keys [target-type cast-opts]}]
  (codegen-cast {:source-type source-type, :target-type target-type :cast-opts cast-opts}))

(defmethod codegen-expr :struct [{:keys [entries]} opts]
  (let [emitted-vals (->> entries
                          (mapv (fn [[k expr]]
                                  (let [k (symbol k)]
                                    {:k k
                                     :val-box (gensym (str (util/symbol->normal-form-symbol k) "-box"))
                                     :emitted-expr (codegen-expr expr opts)}))))

        return-type [:struct (->> emitted-vals
                                  (into {} (map (juxt :k (comp :return-type :emitted-expr)))))]
        map-sym (gensym 'map)]

    {:return-type return-type
     :batch-bindings (conj (->> emitted-vals
                                (mapv (fn [{:keys [val-box]}]
                                        [val-box `(ValueBox.)])))

                           [(-> map-sym (with-tag Map))
                            (->> emitted-vals
                                 (into {} (map (fn [{:keys [k val-box]}]
                                                 [(str k) val-box]))))])

     :children (mapv :emitted-expr emitted-vals)
     :continue (fn [f]
                 `(do
                    ~@(for [{:keys [val-box], {:keys [continue]} :emitted-expr} emitted-vals]
                        (continue (fn [val-type code]
                                    (write-value-code val-type `(.legWriter ~val-box ~(types/col-type->leg val-type)) code))))

                    ~(f return-type map-sym)))}))

(defmethod codegen-call [:get_field :struct] [{[[_ field-types]] :arg-types, :keys [field]}]
  (if-let [val-type (get field-types field)]
    {:return-type val-type
     :continue-call (fn [f [struct-code]]
                      (continue-read f val-type (-> `(.get ~struct-code ~(str field))
                                                    (with-tag ValueReader))))}

    {:return-type :null, :->call-code (constantly nil)}))

(defmethod codegen-call [:get_field :any] [_]
  {:return-type :null, :->call-code (constantly nil)})

(doseq [[op return-code] [[:= 1] [:<> -1]]]
  (defmethod codegen-call [op :struct :struct] [{[[_ l-field-types] [_ r-field-types]] :arg-types}]
    (let [fields (set (keys l-field-types))]
      (if-not (= fields (set (keys r-field-types)))
        {:return-type :bool, :->call-code (constantly (if (= := op) false true))}

        (let [inner-calls (->> (for [field fields]
                                 (MapEntry/create field
                                                  (->> (for [l-val-type (-> (get l-field-types field) types/flatten-union-types)
                                                             r-val-type (-> (get r-field-types field) types/flatten-union-types)]
                                                         (MapEntry/create [l-val-type r-val-type]
                                                                          (if (or (= :null l-val-type) (= :null r-val-type))
                                                                            {:return-type :null, :->call-code (constantly nil)}
                                                                            (codegen-call {:f :=, :arg-types [l-val-type r-val-type]}))))
                                                       (into {}))))
                               (into {}))
              l-sym (gensym 'l-struct)
              r-sym (gensym 'r-struct)]

          {:return-type [:union #{:bool :null}]
           :continue-call (fn [f [l-code r-code]]
                            (let [res-sym (gensym 'res)]
                              `(let [~l-sym ~l-code
                                     ~r-sym ~r-code
                                     ~res-sym
                                     ~(->> (for [[field inner-calls] inner-calls]
                                             (continue-read (fn [l-val-type l-val-code]
                                                              (continue-read (fn [r-val-type r-val-code]
                                                                               (let [{:keys [return-type continue-call ->call-code]} (get inner-calls [l-val-type r-val-type])]
                                                                                 (if continue-call
                                                                                   (continue-call cont-b3-call [l-val-code r-val-code])
                                                                                   (cont-b3-call return-type (->call-code [l-val-code r-val-code])))))
                                                                             (get r-field-types field)
                                                                             (-> `(.get ~r-sym ~(str field))
                                                                                 (with-tag ValueReader))))
                                                            (get l-field-types field)
                                                            (-> `(.get ~l-sym ~(str field))
                                                                (with-tag ValueReader))))
                                           (reduce (fn [l-code r-code]
                                                     `(let [~res-sym ~l-code]
                                                        (if (== -1 ~res-sym)
                                                          -1
                                                          (min ~res-sym ~r-code))))
                                                   1))]
                                 (if (zero? ~res-sym)
                                   ~(f :null nil)
                                   ~(f :bool `(== ~return-code ~res-sym))))))})))))

(defn -patch [^ValueReader l, ^ValueReader r]
  (if (.isNull r)
    l
    r))

(defmethod codegen-call [:_patch :struct :struct] [{[[_ l-ks] [_ r-ks]] :arg-types}]
  (let [flattened-r-types (-> r-ks (update-vals types/flatten-union-types))]
    {:return-type [:struct (into l-ks
                                 (map (fn [[k r-type]]
                                        [k (let [r-types (flattened-r-types k)]
                                             (if-not (contains? r-types :null)
                                               r-type
                                               (let [l-type (get l-ks k :null)]
                                                 ;; if the r-val is null, we take the l-val
                                                 (apply types/merge-col-types l-type (disj r-types :null)))))]))
                                 r-ks)]
     :->call-code (fn [[l r]]
                    (let [l-sym (gensym 'l)
                          r-sym (gensym 'r)]
                      `(let [~l-sym ~l, ~r-sym ~r]
                         ~(-> {}
                              (into (map (fn [k]
                                           (let [k-str (str k)]
                                             [k-str `(get ~l-sym ~k-str)])))
                                    (keys l-ks))
                              (into (map (fn [k]
                                           (let [k-str (str k)]
                                             [k-str
                                              (if (and (contains? l-ks k)
                                                       (get-in flattened-r-types [k :null]))
                                                `(-patch (get ~l-sym ~k-str) (get ~r-sym ~k-str))
                                                `(get ~r-sym ~k-str))])))
                                    (keys r-ks))))))}))

(defmethod codegen-call [:_patch :null :struct] [{[_ [_ r-ks]] :arg-types}]
  {:return-type [:struct r-ks]
   :->call-code (fn [[l r]] r)})

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
  (let [emitted-els (->> elements
                         (into [] (map-indexed (fn [idx el]
                                                 {:idx idx
                                                  :el-box (gensym (str "el" idx))
                                                  :emitted-el (codegen-expr el opts)}))))

        return-type [:list (->> (into #{} (map (comp :return-type :emitted-el)) emitted-els)
                                (apply types/merge-col-types))]]

    {:return-type return-type
     :children (mapv :emitted-el emitted-els)

     :batch-bindings (->> emitted-els
                          (mapv (fn [{:keys [el-box]}]
                                  [el-box `(ValueBox.)])))
     :continue (fn [f]
                 (f return-type
                    `(reify ListValueReader
                       (~'size [_#] ~(count emitted-els))

                       (~'nth [_# idx#]
                        (case idx#
                          ~@(->> emitted-els
                                 (sequence (comp (map-indexed (fn [idx {:keys [el-box], {:keys [continue]} :emitted-el}]
                                                                [idx (continue (fn [return-type code]
                                                                                 (let [leg (types/col-type->leg return-type)]
                                                                                   `(do
                                                                                      ~(write-value-code return-type `(.legWriter ~el-box ~leg) code)
                                                                                      ~el-box))))]))
                                                 cat))))))))}))

(defmethod codegen-expr :set [expr opts]
  (let [{[_list el-type] :return-type, continue-list :continue, :as emitted-expr} (codegen-expr (assoc expr :op :list) opts)
        return-type [:set el-type]]
    (-> emitted-expr
        (assoc :return-type return-type
               :continue (fn [f]
                           (continue-list (fn [_list-type code]
                                            (f return-type code))))))))

(defmethod codegen-call [:nth :list :int] [{[[_ list-el-type] _n-type] :arg-types}]
  (let [return-type (types/merge-col-types list-el-type :null)]
    {:return-type return-type
     :continue-call (fn [f [list-code n-code]]
                      (let [list-sym (gensym 'list)
                            n-sym (gensym 'n)]
                        `(let [~list-sym ~list-code
                               ~n-sym ~n-code]
                           (if (and (>= ~n-sym 0) (< ~n-sym (.size ~list-sym)))
                             (do
                               ~(continue-read f list-el-type `(.nth ~list-sym ~n-sym)))
                             ~(f :null nil)))))}))

(defmethod codegen-call [:nth :any :int] [_]
  {:return-type :null, :->call-code (constantly nil)})

(defmethod codegen-call [:cardinality :list] [_]
  {:return-type :i32
   :->call-code #(do `(.size ~@%))})

(defmethod codegen-call [:length :utf8] [expr]
  (codegen-call (assoc expr :f :character_length)))

(defmethod codegen-call [:length :varbinary] [expr]
  (codegen-call (assoc expr :f :octet_length)))

(defmethod codegen-call [:length :list] [expr]
  (codegen-call (assoc expr :f :cardinality)))

(defmethod codegen-call [:length :set] [_]
  {:return-type :i32
   :->call-code #(do `(count ~@%))})

(defn count-non-empty [m]
  (loop [n 0, xs (vals m)]
    (if (seq xs)
      (if (.isNull ^ValueReader (first xs))
        (recur n (rest xs))
        (recur (inc n) (rest xs)))
      n)))

(defmethod codegen-call [:length :struct] [_]
  {:return-type :i32
   :->call-code #(do `(count-non-empty ~@%))})

(defmethod codegen-call [:array_upper :list :i64] [_]
  {:return-type :i32
   :->call-code (fn [[arr dim]]
                  `(do
                     (when-not (= ~dim 1)
                       (throw (err/runtime-err :xtdb.expression/array-dimension-error
                                               {::err/message "Unsupported: ARRAY_UPPER for dimension != 1"
                                                :dim ~dim})))

                     (.size ~arr)))})

(defn trim-array-view ^xtdb.arrow.ListValueReader [^long trimmed-value-count ^ListValueReader lst]
  (reify ListValueReader
    (size [_] trimmed-value-count)
    (nth [_ idx] (.nth lst idx))))

(defmethod codegen-call [:trim_array :list :int] [{[[_list list-el-type] _n-type] :arg-types}]
  (let [return-type [:list list-el-type]]
    {:return-type return-type
     :continue-call (fn [f [list-code n-code]]
                      (let [list-sym (gensym 'list)
                            n-sym (gensym 'n)]
                        `(let [~list-sym ~list-code
                               ~n-sym (- (.size ~list-sym) ~n-code)]
                           (if (neg? ~n-sym)
                             (throw (err/runtime-err :xtdb.expression/array-element-error
                                                     {::err/message "Data exception - array element error."
                                                      :nlen ~n-sym}))
                             ~(f return-type (-> `(trim-array-view ~n-sym ~list-sym)
                                                 (with-tag ListValueReader)))))))}))

(doseq [[op return-code] [[:= 1] [:<> -1]]]
  (defmethod codegen-call [op :list :list] [{[[_ l-el-type] [_ r-el-type]] :arg-types}]
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
                                 (let [~len-sym (.size ~l-sym)]
                                   (if-not (= ~len-sym (.size ~r-sym))
                                     -1
                                     (loop [~n-sym 0, ~res-sym 1]
                                       (cond
                                         (>= ~n-sym ~len-sym) ~res-sym
                                         (== -1 ~res-sym) -1
                                         :else (recur (inc ~n-sym)
                                                      (min ~res-sym
                                                           (do
                                                             ~(continue-read
                                                               (fn cont-l [l-el-type l-el-code]
                                                                 `(do
                                                                    ~(continue-read (fn cont-r [r-el-type r-el-code]
                                                                                      (let [{:keys [return-type continue-call ->call-code]} (get inner-calls [l-el-type r-el-type])]
                                                                                        (if continue-call
                                                                                          (continue-call cont-b3-call [l-el-code r-el-code])
                                                                                          (cont-b3-call return-type (->call-code [l-el-code r-el-code])))))
                                                                                    r-el-type
                                                                                    `(.nth ~r-sym ~n-sym))))
                                                               l-el-type
                                                               `(.nth ~l-sym ~n-sym)))))))))]
                             (if (zero? ~res-sym)
                               ~(f :null nil)
                               ~(f :bool `(== ~return-code ~res-sym))))))})))

(defn series [^long start, ^long end, ^long step]
  (let [box (ValueBox.)]
    (reify ListValueReader
      (size [_]
        (Math/max (int 0)
                  (int (Math/ceil (/ (Math/subtractExact end start) step)))))

      (nth [_ idx]
        (doto box
          (.writeLong (+ start (* step idx))))))))

(defmethod codegen-call [:generate_series :int :int :int] [_]
  {:return-type [:list :i64]
   :->call-code (fn [[start end step]]
                  `(series (long ~start) (long ~end) (long ~step)))})

(defmethod codegen-call [:= :set :set] [{[[_ _l-el-type] [_ _r-el-type]] :arg-types}]
  (throw (UnsupportedOperationException. "TODO: `=` on sets")))

(defmethod codegen-call [:<> :set :set] [{[[_ _l-el-type] [_ _r-el-type]] :arg-types}]
  (throw (UnsupportedOperationException. "TODO: `<>` on sets")))

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
  (let [field (types/col-type->field return-type)]
    (if (= (.getType field) #xt.arrow/type :union)
      (let [writer-syms (->> (.getChildren field)
                             (into {} (map (juxt types/field->col-type (fn [_] (gensym 'out-writer))))))]
        {:writer-bindings (into [out-writer-sym `(vw/->writer ~out-vec-sym)]
                                (mapcat (fn [[value-type writer-sym]]
                                          [writer-sym `(.legWriter ~out-writer-sym ~(types/->arrow-type value-type))]))
                                writer-syms)

         :write-value-out! (fn [value-type code]
                             (write-value-code value-type (get writer-syms value-type) code))})

      {:writer-bindings [out-writer-sym `(vw/->writer ~out-vec-sym)]
       :write-value-out! (fn [value-type code]
                           (write-value-code value-type out-writer-sym code))})))

(defn- wrap-zone-id-cache-buster [f]
  (fn [expr opts]
    (f expr (assoc opts :zone-id *default-tz*))))

(defn arithmetic-ex->runtime-ex [^Throwable cause]
  (let [message (.getMessage cause)]
    (cond
      (and message (str/index-of message "/ by zero"))
      (err/runtime-err ::division-by-zero {::err/message "data exception - division by zero"} cause)

      (and message (str/index-of message "overflow"))
      (err/runtime-err ::overflow-error {::err/message "data exception - overflow error"} cause)

      :else
      (err/runtime-err ::unknown-arithmetic-error {::err/message "data exception - arithmetic exception"} cause))))


(def ^:private emit-projection
  "NOTE: we macroexpand inside the memoize on the assumption that
   everything outside yields the same result on the pre-expanded expr - this
   assumption wouldn't hold if macroexpansion created new variable exprs, for example.
   macroexpansion is non-deterministic (gensym), so busts the memo cache."
  (-> (fn [expr opts]
        (let [expr (prepare-expr expr)
              {:keys [return-type continue] :as emitted-expr} (codegen-expr expr opts)
              {:keys [writer-bindings write-value-out!]} (write-value-out-code return-type)]
          (assert return-type (pr-str expr))
          {:!projection-fn (delay
                             (-> `(fn [~(-> rel-sym (with-tag RelationReader))
                                       ~(-> schema-sym (with-tag IPersistentMap))
                                       ~(-> args-sym (with-tag RelationReader))
                                       ~(-> out-vec-sym (with-tag ValueVector))]
                                    (let [~@(batch-bindings emitted-expr)
                                          ~@writer-bindings
                                          row-count# (.getRowCount ~rel-sym)]
                                      (dotimes [~idx-sym row-count#]
                                        ~(continue (fn [t c]
                                                     (write-value-out! t c))))))
                                 #_(doto clojure.pprint/pprint) ; <<no-commit>>
                                 #_(->> (binding [*print-meta* true]))
                                 eval))

           :return-type return-type}))
      (util/lru-memoize) ; <<no-commit>>
      wrap-zone-id-cache-buster))

(defn ->param-types [^RelationReader args]
  (->> args
       (into {} (map (fn [^VectorReader col]
                       (MapEntry/create
                        (symbol (.getName col))
                        (types/field->col-type (.getField col))))))))

(defn ->expression-projection-spec ^xtdb.operator.ProjectionSpec [col-name expr {:keys [col-types param-types]}]
  (let [;; HACK - this runs the analyser (we discard the emission) to get the widest possible out-type.
        widest-out-type (-> (emit-projection expr {:param-types param-types
                                                   :var->col-type col-types})
                            :return-type)]

    (reify ProjectionSpec
      (getColumnName [_] col-name)

      (getColumnType [_] widest-out-type)

      (project [_ allocator in-rel schema args]
        (let [in-rel (RelationReader/from in-rel)
              args (RelationReader/from args)
              var->col-type (->> (seq in-rel)
                                 (into {} (map (fn [^VectorReader iv]
                                                 [(symbol (.getName iv))
                                                  (types/field->col-type (.getField iv))]))))

              {:keys [return-type !projection-fn]} (emit-projection expr {:param-types (->param-types args)
                                                                          :var->col-type var->col-type})
              row-count (.getRowCount in-rel)]
          (util/with-close-on-catch [out-vec (-> (types/col-type->field col-name return-type)
                                                 (.createVector allocator))]
            (doto out-vec
              (.setInitialCapacity row-count)
              (.allocateNew))
            (try
              (@!projection-fn in-rel schema args out-vec)
              (catch ArithmeticException e
                (throw (arithmetic-ex->runtime-ex e)))
              (catch NumberFormatException e
                (throw (err/runtime-err :xtdb.expression/number-format-error {::err/message (.getMessage e)} e)))
              (catch ClassCastException e
                (throw (err/runtime-err :xtdb.expression/class-cast-exception {::err/message (.getMessage e)} e)))
              (catch IllegalArgumentException e
                (throw (err/illegal-arg :xtdb.expression/illegal-argument-exception {::err/message (.getMessage e)} e))))
            (.setValueCount out-vec row-count)
            (vr/vec->reader out-vec)))))))

(defn ->expression-selection-spec ^SelectionSpec [expr input-types]
  (let [projector (->expression-projection-spec "select" {:op :call, :f :boolean, :args [expr]} input-types)]
    (reify SelectionSpec
      (select [_ al in-rel schema args]
        (with-open [selection (.project projector al in-rel schema args)]
          (let [res (IntStream/builder)]
            (dotimes [idx (.getValueCount selection)]
              (when (.getBoolean selection idx)
                (.add res idx)))
            (.toArray (.build res))))))))
