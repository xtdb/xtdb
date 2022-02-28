(ns core2.trip
  (:require [clojure.spec.alpha :as s])
  (:import clojure.lang.IPersistentMap))

;; Internal triple store.

(defprotocol DbConnection
  (-transact [this tx-ops])
  (-db [this]))

(defprotocol Db
  (-datoms [this index components]))

(defn logic-var? [x]
  (and (simple-symbol? x) (= \? (first (name x)))))

(defn source-var? [x]
  (= '$ x))

(defn rules-var? [x]
  (= '% x))

(defn blank-var? [x]
  (= '_ x))

(s/def ::query (s/and (s/conformer identity vec)
                      (s/cat :find-spec ::find-spec
                             :return-map (s/? ::return-map)
                             :with-clause (s/? ::with-clause)
                             :inputs (s/? ::inputs)
                             :where-clauses (s/? ::where-clauses))))

(s/def ::find-spec (s/cat :find #{:find}
                          :find-spec (s/alt :find-rel ::find-rel
                                            :find-coll ::find-coll
                                            :find-tuple ::find-tuple
                                            :find-scalar ::find-scalar)))

(s/def ::return-map (s/alt :return-keys ::return-keys
                           :return-syms ::return-syms
                           :return-strs ::return-strs))

(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-coll (s/tuple ::find-elem '#{...}))
(s/def ::find-scalar (s/cat :find-elem ::find-elem :period '#{.}))
(s/def ::find-tuple (s/coll-of ::find-elem :min-count 1))

(s/def ::find-elem (s/or :variable ::variable :aggregate ::aggregate))

(s/def ::return-keys (s/cat :keys #{:keys} :symbols (s/+ symbol?)))
(s/def ::return-syms (s/cat :syms #{:syms} :symbols (s/+ symbol?)))
(s/def ::return-strs (s/cat :strs #{:strs} :symbols (s/+ symbol?)))

(s/def ::aggregate (s/cat :aggregate-fn-name ::plain-symbol :fn-args (s/+ ::fn-arg)))
(s/def ::fn-arg (s/or :variable ::variable :constant ::constant :src-var ::src-var))

(s/def ::with-clause (s/cat :with #{:with} :variables (s/+ ::variable)))
(s/def ::where-clauses (s/cat :where #{:where} :clauses (s/+ ::clause)))

(s/def ::inputs (s/cat :in #{:in} :inputs (s/+ (s/or :src-var ::src-var :binding ::binding :rules-var ::rules-var))))
(s/def ::src-var source-var?)
(s/def ::variable logic-var?)
(s/def ::rules-var rules-var?)
(s/def ::blank-var blank-var?)

(s/def ::plain-symbol (s/and symbol? (complement (some-fn source-var? logic-var? rules-var?))))

(s/def ::and-clause (s/cat :and '#{and} :clauses (s/+ ::clause)))
(s/def ::expression-clause (s/or :data-pattern ::data-pattern
                                 :pred-expr ::pred-expr
                                 :fn-expr ::fn-expr
                                 :rule-expr ::rule-expr))

(s/def ::rule-expr (s/cat :src-var (s/? ::src-var)
                          :rule-name ::rule-name
                          :args (s/+ (s/or :variable ::variable
                                           :constant ::constant
                                           :blank-var ::blank-var))))

(s/def ::not-clause (s/cat :src-var (s/? ::src-var)
                           :not '#{not}
                           :clauses (s/+ ::clause)))

(s/def ::not-join-clause (s/cat :src-var (s/? ::src-var)
                                :not-join '#{not-join}
                                :args (s/coll-of ::variable :kind vector? :min-count 1)
                                :clauses (s/+ ::clause)))

(s/def ::or-clause (s/cat :src-var (s/? ::src-var)
                          :or '#{or}
                          :clauses (s/+ (s/or :clause ::clause
                                              :and-clause ::and-clause))))

(s/def ::or-join-clause (s/cat :src-var (s/? ::src-var)
                               :or-join '#{or-join}
                               :args (s/tuple ::rule-vars)
                               :clauses (s/+ (s/or :clause ::clause
                                                   :and-clause ::and-clause))))

(s/def ::rule-vars  (s/cat :bound-vars (s/? (s/coll-of ::variable :kind vector? :min-count 1))
                           :free-vars (s/* ::variable)))

(s/def ::clause (s/or :not-clause ::not-clause
                      :not-join-clause ::not-join-clause
                      :or-clause ::or-clause
                      :or-join-clause ::or-join-clause
                      :expression-clause ::expression-clause))

(s/def ::data-pattern (s/and (s/conformer identity vec)
                             (s/cat :src-var (s/? ::src-var)
                                    :pattern (s/+ (s/or :variable ::variable
                                                        :constant ::constant
                                                        :blank-var ::blank-var)))))

(s/def ::constant (s/and any? #(not (or (symbol? %) (list? %)))))

(s/def ::pred-expr (s/tuple (s/cat :pred ::pred
                                   :args (s/* ::fn-arg))))
(s/def ::pred ::plain-symbol)

(s/def ::fn-expr (s/tuple (s/cat :fn ::fn
                                 :args (s/* ::fn-arg))
                          ::binding))
(s/def ::fn ::plain-symbol)

(s/def ::binding (s/or :bind-scalar ::bind-scalar
                       :bind-tuple ::bind-tuple
                       :bind-coll ::bind-coll
                       :bind-rel ::bind-rel))

(s/def ::bind-scalar ::variable)
(s/def ::bind-tuple (s/coll-of (s/or :variable ::variable
                                     :blank-var ::blank-var)
                               :kind vector? :min-count 1))
(s/def ::bind-coll (s/tuple ::variable '#{...}))
(s/def ::bind-rel (s/tuple (s/coll-of (s/or :variable ::variable
                                            :blank-var '#{_})
                                      :kind vector? :min-count 1)))

(s/def ::rule (s/coll-of
               (s/and (s/conformer identity vec)
                      (s/cat :rule-head ::rule-head
                             :clauses (s/+ ::clause)))
               :kind vector?
               :min-count 1))
(s/def ::rule-head (s/and list? (s/cat :rule-name ::rule-name
                                       :rule-vars ::rule-vars)))
(s/def ::unqualified-plain-symbol simple-symbol?)
(s/def ::rule-name (s/and ::unqualified-plain-symbol (complement '#{and or or-join not not-join})))

;; Query runtime

(declare datoms)

(defn data-pattern [$ e a v]
  (for [{:keys [e a v]}
        (if (= ::unbound e)
          (if (= ::unbound a)
            (if (= ::unbound v)
              (datoms $ :aev)
              (datoms $ :vae v))
            (if (= ::unbound v)
              (datoms $ :aev a)
              (datoms $ :ave a v)))
          (if (= ::unbound a)
            (if (= ::unbound v)
              (datoms $ :eav e)
              (for [datom (datoms $ :eav e)
                    :when (= v (:v datom))]
                datom))
            (if (= ::unbound v)
              (datoms $ :eav e a)
              (datoms $ :eav e a v))))]
    [e a v]))

(defn rule [$ rule-ctx leg-fns args]
  (for [leg-fn leg-fns
        :let [rule-leg-key [leg-fn args]]
        :when (not (contains? rule-ctx rule-leg-key))
        x (apply leg-fn $ (conj rule-ctx rule-leg-key) args)]
    x))

(defn -sum [vals]
  (reduce + vals))

(defn -avg [vals]
  (/ (reduce + vals) (count vals)))

(defn -min [vals]
  (reduce min vals))

(defn -max [vals]
  (reduce max vals))

(defn -count [vals]
  (count vals))

(defn -count-distinct [vals]
  (count (distinct vals)))

;; Query compiler

(def ^:private ^:dynamic *allow-unbound?* true)

(defmacro ^:private lvar [x]
  (if (or (not (logic-var? x))
          (contains? &env x)
          (not *allow-unbound?*))
    x
    ::unbound))

(defmacro ^:private lvars-in-scope-env []
  (let [vars (filterv logic-var? (keys &env))]
    `(zipmap '~vars ~vars)))

(defmacro ^:private assert-bound-lvar [x]
  `(let [x# ~x]
     (if (= ::unbound x#)
       (throw (IllegalArgumentException. (str "not bound: " '~x)))
       x#)))

(defmacro ^:private can-unify? [binding pattern]
  (cond (blank-var? pattern)
        true

        (contains? &env pattern)
        `(let [pattern# ~pattern]
           (or (and *allow-unbound?* (= ::unbound pattern#)) (= ~binding pattern#)))

        (vector? pattern)
        (let [tmp-sym (gensym 'tmp)]
          `(let [~tmp-sym ~binding]
             (and (vector? ~tmp-sym)
                  ~@(for [[_ unify-group] (group-by second (map-indexed vector pattern))
                          :when (> (count unify-group) 1)]
                      `(= ~@(for [[idx] unify-group]
                              `(nth ~tmp-sym ~idx))))
                  ~@(for [[idx pattern] (map-indexed vector pattern)]
                      `(can-unify? (nth ~tmp-sym ~idx) ~pattern)))))

        :else
        true))

(defn- compiles? [parent-vars form]
  (try
    (eval `(fn [~@parent-vars]
             ~form))
    true
    (catch Exception sfinae
      false)))

(defmacro ^:private ^{:style/indent 1} for-deps [seq-exprs body-expr]
  (let [seq-exprs (concat `[_# [nil]] seq-exprs)]
    (if (compiles? (keys &env)
                   `(for [~@seq-exprs]
                      true))
      `(for [~@seq-exprs] ~body-expr)
      (loop [bind-groups (partition-all 2 seq-exprs)
             acc []]
        (if (empty? bind-groups)
          `(for [~@acc]
             ~body-expr)
          (if-let [bind-group (some (fn [bind-group]
                                      (when (compiles? (keys &env)
                                                       `(for [~@(into acc bind-group)]
                                                          true))
                                        bind-group))
                                    bind-groups)]
            (recur (remove #{bind-group} bind-groups) (into acc bind-group))
            (throw (IllegalArgumentException. "Circular dependency."))))))))

(defn- lvar-ref [x]
  (list 'lvar x))

(defn- assert-new-scope [parent-vars body]
  (eval `(fn [~@parent-vars] ~@body))
  body)

(defn- assert-bound-lvar-ref [x]
  (list 'assert-bound-lvar x))

(defmulti ^:private datalog->clj (fn [datalog ctx] (first datalog)))

(defn- clauses->clj [ctx clauses]
  (mapcat #(datalog->clj % ctx) clauses))

(defn- wrap-with-binding [[binding-type binding] form]
  (let [binding-sym (gensym 'binding)]
    (case binding-type
      :bind-scalar `[:let [~binding-sym ~form]
                     :when (can-unify? ~binding-sym ~binding)
                     :let [~binding ~binding-sym]]
      :bind-tuple (let [binding (mapv second binding)]
                    `[:let [~binding-sym ~form]
                      :when (can-unify? ~binding-sym ~binding)
                      :let [~binding ~binding-sym]])
      :bind-coll (let [binding (first binding)]
                   `[~binding-sym ~form
                     :when (can-unify? ~binding-sym ~binding)
                     :let [~binding ~binding-sym]])
      :bind-rel (let [binding (mapv second (first binding))]
                  `[~binding-sym ~form
                    :when (can-unify? ~binding-sym ~binding)
                    :let [~binding ~binding-sym]]))))

(defn- pattern->bind-rel [pattern]
  [:bind-rel [(vec (for [pattern pattern]
                     (if (logic-var? pattern)
                       [:variable pattern]
                       [:blank-var '_])))]])

(defmethod datalog->clj :expression-clause [[_ clause] ctx]
  (datalog->clj clause ctx))

(defmethod datalog->clj :data-pattern [[_ {:keys [src-var pattern]}] ctx]
  (let [[e a v] (map second pattern)]
    (wrap-with-binding
     (pattern->bind-rel [e a v])
     `(data-pattern ~(or src-var '$) ~@(map lvar-ref [e a v])))))

(defmethod datalog->clj :rule-expr [[_ {:keys [src-var rule-name args]}] {:keys [name->rules] {:keys [rule-table-sym]} :symbols :as ctx}]
  (let [args (map second args)
        [bound-args free-args] (-> name->rules
                                   (get-in [rule-name 0 :rule-head :rule-vars :bound-vars])
                                   (count)
                                   (split-at args))
        out-args (vec free-args)]
    (wrap-with-binding
     (pattern->bind-rel out-args)
     `(~rule-name ~(or src-var '$) ~rule-table-sym ~@(map assert-bound-lvar-ref bound-args) ~@(map lvar-ref free-args)))))

(defmethod datalog->clj :pred-expr [[_ [{:keys [pred args]}]] _]
  [:when `(~pred ~@(map (comp assert-bound-lvar-ref second) args))])

(defmethod datalog->clj :fn-expr [[_ [{:keys [fn args]} binding]] ctx]
  (wrap-with-binding binding `(~fn ~@(map (comp assert-bound-lvar-ref second) args))))

(defmethod datalog->clj :not-clause [[_ {:keys [src-var clauses]}] ctx]
  `[:when (let [~'$ ~(or src-var '$)]
            (binding [*allow-unbound?* false]
              (empty? (for-deps ~(clauses->clj ctx clauses)
                        true))))])

(defmethod datalog->clj :not-join-clause [[_ {:keys [src-var clauses args]}] ctx]
  `[:when (let [~'$ ~(or src-var '$)]
            ~@(map assert-bound-lvar-ref args)
            ~(assert-new-scope
              (cons '$ args)
              `(empty? (for-deps ~(clauses->clj ctx clauses)
                         true))))])

(defmethod datalog->clj :or-clause [[_ {:keys [src-var clauses]}] ctx]
  (let [out-args (filterv logic-var? (distinct (flatten clauses)))]
    `[{:syms ~out-args :or ~(zipmap out-args (repeat ::unbound))}
      (let [~'$ ~(or src-var '$)]
        (concat ~@(for [[clause-type clause] clauses]
                    `(for-deps ~(case clause-type
                                  :clause (clauses->clj ctx [clause])
                                  :and (clauses->clj ctx (:clauses clause)))
                       (lvars-in-scope-env)))))]))

(defmethod datalog->clj :or-join-clause [[_ {:keys [src-var clauses] {:keys [bound-vars free-vars]} :args}] ctx]
  (let [out-args (vec free-vars)]
    (wrap-with-binding
     (pattern->bind-rel out-args)
     `(let [~'$ ~(or src-var '$)]
        ~@(map assert-bound-lvar-ref bound-vars)
        (binding [*allow-unbound?* true]
          (vec (concat ~@(for [[clause-type clause] clauses]
                           (assert-new-scope
                            (cons '$ (concat bound-vars free-vars))
                            `(for-deps ~(case clause-type
                                          :clause (clauses->clj ctx [clause])
                                          :and (clauses->clj ctx (:clauses clause)))
                               ~out-args))))))))))

(defn- rule-leg-name [rule-name idx]
  (symbol (str rule-name "-" idx)))

(defn- rules->clj [{:keys [name->rules] {:keys [rule-table-sym]} :symbols :as ctx}]
  (->> (for [[rule-name rule-legs] name->rules
             :let [idx->rule-leg (zipmap (range (count rule-legs)) rule-legs)
                   rule-leg-refs (mapv #(rule-leg-name rule-name %) (keys idx->rule-leg))]]
         (->> (for [[idx {:keys [clauses]
                          {{:keys [bound-vars free-vars]} :rule-vars} :rule-head}] idx->rule-leg
                    :let [arg-vars (concat bound-vars free-vars)]]
                `(~(rule-leg-name rule-name idx) [~'$ ~rule-table-sym ~@arg-vars]
                  (do ~@(map assert-bound-lvar-ref bound-vars)
                      (for-deps [~@(clauses->clj ctx clauses)]
                        ~(vec free-vars)))))
              (cons `(~rule-name [db# rule-table# & args#]
                      (rule db# rule-table# ~rule-leg-refs args#)))))
       (reduce into [])))

(def ^:private aggregate-fn-name->built-in-fn-name
  {'sum `-sum 'avg `-avg 'min `-min 'max `-max 'count `-count 'count-distinct `-count-distinct})

(defn- aggregates->clj [find-elements {{:keys [group-sym]} :symbols :as ctx}]
  (for [[idx [find-elem-type find-elem]] (map-indexed vector find-elements)]
    (case find-elem-type
      :variable find-elem
      :aggregate (let [{:keys [aggregate-fn-name fn-args]} find-elem]
                   `(~(get aggregate-fn-name->built-in-fn-name aggregate-fn-name aggregate-fn-name)
                     ~@(map second (butlast fn-args)) (map #(nth % ~idx) ~group-sym))))))

(defn- find-spec-elements [find-spec]
  (let [[find-type find-spec] (:find-spec find-spec)]
    (case find-type
      (:find-rel :find-tuple) find-spec
      :find-coll [(first find-spec)]
      :find-scalar [(:find-elem find-spec)])))

(defn- find-element-projection [[find-elem-type find-elem]]
  (case find-elem-type
    :variable find-elem
    :aggregate (second (last (:fn-args find-elem)))))

(defn- group-idxs [find-elements]
  (vec (for [[idx [find-elem-type _]] (map-indexed vector find-elements)
             :when (= :variable find-elem-type)]
         idx)))

(defn- wrap-with-find-spec [find-spec {{:keys [group-sym]} :symbols :as ctx} seq-exprs-form]
  (let [find-elements (find-spec-elements find-spec)
        aggregates? (some #{:aggregate} (map first find-elements))
        projected-vars (mapv find-element-projection find-elements)
        form `(for-deps ~seq-exprs-form
                ~projected-vars)]
    (if aggregates?
      `(->> ~form
            (group-by #(mapv % ~(group-idxs find-elements)))
            (map (fn [[~projected-vars ~group-sym]]
                   [~@(aggregates->clj find-elements ctx)])))
      form)))

(defn- wrap-with-return-map [return-map _ query-form]
  (if-let [tuple-keys (when-let [[return-map-type {:keys [symbols]}] return-map]
                        (not-empty (map (case return-map-type
                                          :return-keys keyword
                                          :return-syms identity
                                          :return-strs str)
                                        symbols)))]

    `(map (partial zipmap '~tuple-keys) ~query-form)
    query-form))

(defn- src-var-top-level-bindings [inputs {{:keys [inputs-sym]} :symbols :as ctx}]
  (if-let [inputs (:inputs inputs)]
    (->> (for [[idx [in-type in]] (map-indexed vector inputs)
               :when (= :src-var in-type)]
           `[~in (nth ~inputs-sym ~idx)])
         (reduce into []))
    `[~'$ (nth ~inputs-sym 0)]))

(defn- input-bindings [inputs {{:keys [inputs-sym]} :symbols :as ctx}]
  (when-let [inputs (:inputs inputs)]
    (->> (for [[idx [in-type in]] (map-indexed vector inputs)
               :when (= :binding in-type)]
           (wrap-with-binding in `(nth ~inputs-sym ~idx)))
         (reduce into []))))

(defn- normalize-query [query]
  (if (vector? query)
    query
    (vec (for [k [:find :keys :syms :strs :with :in :where]
               :when (contains? query k)
               x (cons k (get query k))]
           x))))

(defn- query->clj [query rules]
  (let [query (->> (normalize-query query)
                   (s/assert ::query)
                   (s/conform ::query))
        name->rules (some->> rules
                             (s/assert ::rule)
                             (s/conform ::rule)
                             (group-by (comp :rule-name :rule-head)))
        rule-table-sym (gensym 'rule-ctx)
        inputs-sym (gensym 'inputs)
        group-sym (gensym 'group)
        ctx {:symbols {:rule-table-sym rule-table-sym
                       :inputs-sym inputs-sym
                       :group-sym group-sym}
             :name->rules name->rules}
        {:keys [find-spec return-map with-clause inputs where-clauses]} query]
    `(fn [& ~inputs-sym]
       (let [~@(src-var-top-level-bindings inputs ctx)
             ~rule-table-sym #{}]
         (letfn ~(rules->clj ctx)
           ~(->> `[~@(input-bindings inputs ctx)
                   ~@(clauses->clj ctx (:clauses where-clauses))]
                 (wrap-with-find-spec find-spec ctx)
                 (wrap-with-return-map return-map ctx)))))))

(def ^:private memo-compile-query
  (memoize
   (fn [query rules]
     (eval (query->clj query rules)))))

;; Transactions

(s/def :db/id some?)
(s/def :db/tx-op (s/or :add (s/cat :op #{:db/add} :e :db/id :a keyword? :v any?)
                       :add-entity (s/and (s/map-of keyword? any?)
                                          (s/keys :req [:db/id]))
                       :retract (s/cat :op #{:db/retract} :e :db/id :a keyword? :v any?)
                       :retract-entity (s/cat :op #{:db/retractEntity} :e :db/id)
                       :cas (s/cat :op #{:db/cas} :e :db/id :a keyword? :old-v any? :new-v any?)
                       :fn (s/cat :fn symbol? :args (s/* any?))))

(defn- add-entity-ops [entity]
  (let [e (:db/id entity)]
    (for [[a v] entity
          v (if (set? v)
              v
              #{v})]
      [:db/add e a v])))

(defn- retract-entity-ops [db e]
  (for [{:keys [e a v]} (datoms db :eav e)]
    [:db/retract e a v]))

(defn- cas-ops [db e a old-v new-v]
  (if (seq (datoms db :eav e a old-v))
    [[:db/retract e a old-v]
     [:db/add e a new-v]]
    []))

(defn- flatten-tx-ops [db tx-ops]
  (vec (for [tx-op tx-ops
             :let [_ (s/assert :db/tx-op tx-op)
                   [op-type {:keys [e a v] :as conformed-tx-op}] (s/conform :db/tx-op tx-op)]
             tx-op (case op-type
                     :add [tx-op]
                     :add-entity (add-entity-ops tx-op)
                     :retract [tx-op]
                     :retract-entity (retract-entity-ops db e)
                     :cas (let [{:keys [e a old-v new-v]} conformed-tx-op]
                            (cas-ops db e a old-v new-v))
                     :fn (let [{:keys [fn args]} conformed-tx-op]
                           (flatten-tx-ops db (apply fn db args))))]
         tx-op)))

;; API

(defn qseq [{:keys [query args]}]
  (let [{:keys [inputs]} (s/conform ::query (normalize-query query))
        inputs (mapv second (:inputs inputs))
        {rules '%} (zipmap inputs args)]
    (apply (memo-compile-query query rules) args)))

(defn q [query & inputs]
  (let [{:keys [find-spec]} (s/conform ::query (normalize-query query))
        [find-type find-spec] (:find-spec find-spec)
        result (qseq {:query query :args inputs})]
    (case find-type
      :find-rel (set result)
      :find-coll (mapv first result)
      :find-scalar (ffirst result)
      :find-tuple (first result))))

(defn datoms [db index & components]
  (-datoms db index components))

(defn entity [db eid]
  (reduce
   (fn [acc {:keys [e a v]}]
     (update acc a (fn [x]
                     (cond
                       (set? x)
                       (conj x v)

                       (some? x)
                       (conj #{} x v)

                       :else
                       v))))
   {}
   (datoms db :eav eid)))

(defn db [conn]
  (-db conn))

(defn transact [conn tx-ops]
  (let [db (db conn)
        tx-ops (flatten-tx-ops db tx-ops)]
    {:db-before db
     :db-after (-transact conn tx-ops)
     :tx-data (vec (for [[op e a v] tx-ops]
                     {:e e :a a :v v :added (= op :db/add)}))}))

;; Default map implementation

(def ^:private tx-op->fn
  {:db/add (fnil conj #{})
   :db/retract disj})

(def ^:private index->keys
  {:eav [:e :a :v]
   :aev [:a :e :v]
   :ave [:a :v :e]
   :vae [:v :a :e]})

(extend-type IPersistentMap
  DbConnection
  (-db [this] this)

  (-transact [this tx-ops]
    (reduce
     (fn [this [op-type e a v]]
       (let [f (get tx-op->fn op-type)]
         (-> this
             (update-in [:eav e a] f v)
             (update-in [:aev a e] f v)
             (update-in [:ave a v] f e)
             (update-in [:vae v a] f e))))
     this
     tx-ops))

  Db
  (-datoms [this index [x y z :as components]]
    (if-let [idx (get this index)]
      (let [ks (index->keys index)]
        (case (count components)
          0 (for [[x ys] idx
                  [y zs] ys
                  z zs]
              (zipmap ks [x y z]))
          1 (for [[y zs] (get-in idx components)
                  z zs]
              (zipmap ks [x y z]))
          2 (for [z (get-in idx components)]
              (zipmap ks [x y z]))
          3 (when (get-in idx components)
              [(zipmap ks (vec components))])))
      (throw (IllegalArgumentException. (str "unknown index: " index))))))

(comment

  (let [db (->> '[{:db/id :john :parent :douglas}
                  {:db/id :bob :parent :john}
                  {:db/id :ebbon :parent :bob}]
                (transact {})
                :db-after)]
    (= #{[:ebbon :bob]
         [:bob :john]
         [:john :douglas]
         [:bob :douglas]
         [:ebbon :john]
         [:ebbon :douglas]}

       (q '{:find [?a ?b]
            :in [$ %]
            :where [(ancestor ?a ?b)]}
          db
          '[[(ancestor ?a ?b)
             [?a :parent ?b]]
            [(ancestor ?a ?b)
             [?a :parent ?c]
             (ancestor ?c ?b)]])))

  (let [db (->> '[{:db/id :a :edge :b}
                  {:db/id :b :edge :c}
                  {:db/id :c :edge :d}
                  {:db/id :d :edge :a}]
                (transact {})
                :db-after)]
    (= #{[:a :a]
         [:a :d]
         [:a :c]
         [:a :b]
         [:b :a]
         [:b :d]
         [:b :c]
         [:b :b]
         [:c :a]
         [:c :d]
         [:c :c]
         [:c :b]
         [:d :b]
         [:d :c]
         [:d :d]
         [:d :a]}

       (q '{:find [?x ?y]
            :in [$ %]
            :where [(path ?x ?y)]}
          db
          '[[(path ?x ?y)
             [?x :edge ?y]]
            [(path ?x ?y)
             [?x :edge ?z]
             (path ?z ?y)]])))

  (= #{[6 1 3 4 2]}
     (q '[:find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
          :with ?monster
          :in [[?monster ?heads]]]
        [["Cerberus" 3]
         ["Medusa" 1]
         ["Cyclops" 1]
         ["Chimera" 1]]))

  (= #{[1] [5]}
     (q '[:find ?x
          :in [[?x ?x]]]
        [[1 1]
         [1 2]
         [5 5]]))

  (= [3 4 5]
     (q '[:find [?z ...]
          :in [?x ...]
          :where
          [(inc ?y) ?z]
          [(inc ?x) ?y]]
        [1 2 3]))

  (= 55
     (q '[:find ?f .
          :in $ % ?n
          :where (fib ?n ?f)]
        {}
        '[[(fib [?n] ?f)
           [(<= ?n 1)]
           [(identity ?n) ?f]]
          [(fib [?n] ?f)
           [(> ?n 1)]
           [(- ?n 1) ?n1]
           [(- ?n 2) ?n2]
           (fib ?n1 ?f1)
           (fib ?n2 ?f2)
           [(+ ?f1 ?f2) ?f]]]
        10)))
