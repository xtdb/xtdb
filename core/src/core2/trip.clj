(ns core2.trip
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.spec.alpha :as s]))

;; Internal triple store.

(defn logic-var? [x]
  (and (symbol? x) (str/starts-with? (name x) "?")))

(defn source-var? [x]
  (= '$ x))

(defn rules-var? [x]
  (= '% x))

(s/def :db/id some?)
(s/def :db/logic-var logic-var?)
(s/def :db/tx-op (s/or :entity (s/and (s/map-of keyword? any?)
                                      (s/keys :req [:db/id]))
                       :add (s/cat :op #{:db/add} :e :db/id :a keyword? :v any?)
                       :retract (s/cat :op #{:db/retract} :e :db/id :a keyword? :v any?)
                       :retract-entity (s/cat :op #{:db/retractEntity} :e :db/id)
                       :cas (s/cat :op #{:db/cas} :e :db/id :a keyword? :old-v any? :new-v any?)
                       :fn (s/cat :fn symbol? :args (s/* any?))))

(s/def :db.query/find (s/coll-of :db/logic-var :kind vector? :min-count 1))
(s/def :db.query.where.clause/triple (s/and vector? (s/cat :e any? :a (some-fn keyword? logic-var?) :v any?)))
(s/def :db.query.where.clause/rule (s/and list? (s/cat :rule-name symbol? :args (s/* any?))))
(s/def :db.query.where.clause/pred (s/tuple (s/coll-of any? :kind list? :min-count 1)))
(s/def :db.query.where.clause/fn (s/tuple (s/coll-of any? :kind list? :min-count 1) :db/logic-var))
(s/def :db.query.where/clause (s/or :triple :db.query.where.clause/triple
                                    :rule :db.query.where.clause/rule
                                    :pred :db.query.where.clause/pred
                                    :fn :db.query.where.clause/fn))
(s/def :db.query/where (s/coll-of :db.query.where/clause :kind vector? :min-count 1))
(s/def :db.query/in (s/coll-of (some-fn logic-var? source-var? rules-var?) :kind vector?))
(s/def :db.query/keys (s/coll-of symbol? :kind vector? :min-count 1))

(s/def :db/query (s/keys :req-un [:db.query/where :db.query/find]
                         :opt-un [:db.query/in :db.query/keys]))

(s/def :db.query.rule/head (s/spec (s/cat :rule-name symbol?
                                          :bound-vars (s/? (s/coll-of :db/logic-var :kind vector?))
                                          :free-vars (s/* :db/logic-var))))
(s/def :db.query/rule (s/cat :rule-head :db.query.rule/head
                             :rule-body (s/+ :db.query.where/clause)))
(s/def :db.query/rules (s/coll-of :db.query/rule :kind vector? :min-count 1))

(defn- add-triple [db [e a v]]
  (let [conj' (fnil conj #{})]
    (-> db
        (update-in [:eav e a] conj' v)
        (update-in [:aev a e] conj' v)
        (update-in [:ave a v] conj' e)
        (update-in [:vae v a] conj' e))))

(defn- retract-triple [db [e a v]]
  (-> db
      (update-in [:eav e a] disj v)
      (update-in [:aev a e] disj v)
      (update-in [:ave a v] disj e)
      (update-in [:vae v a] disj e)))

(defn- entity-triples [{:keys [eav] :as db} eid]
   (for [[a vs] (get eav eid)
         v vs]
     [eid a v]))

(defn- retract-entity [db eid]
  (reduce
   retract-triple
   db
   (entity-triples db eid)))

(defn- entity->triples [entity]
  (let [e (:db/id entity)]
    (for [[a v] entity
          v (if (set? v)
              v
              #{v})]
      [e a v])))

(defn- add-entity [db entity]
  (reduce add-triple db (entity->triples entity)))

(def ^:private index->keys
  {:eav [:e :a :v]
   :aev [:a :e :v]
   :ave [:a :v :e]
   :vae [:v :a :e]})

(defn datoms [db index & [x y z :as components]]
  (if-let [idx (get db index)]
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
    (throw (IllegalArgumentException. (str "unknown index: " index)))))

(defmacro lvar [x]
  (if (or (not (logic-var? x))
          (contains? &env x))
    x
    ::unbound))

(defn triple [$ e a v]
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

(defn rule [rule-ctx leg-fns args]
  (for [leg-fn leg-fns
        :let [rule-leg-key [leg-fn args]]
        :when (not (contains? rule-ctx rule-leg-key))
        x (apply leg-fn (conj rule-ctx rule-leg-key) args)]
    x))

(def ^:private rule-ctx-sym (gensym 'rule-ctx))

(defn- logic-var-or-blank [x]
  (if (logic-var? x)
    x
    '_))

(defn- lvar-ref [x]
  (list 'lvar x))

(defn- expr-with-lvar-refs [x]
  (w/postwalk (fn [x]
                (if (logic-var? x)
                  (lvar-ref x)
                  x)) x))

(defn- clauses->clj [clauses]
  (->> (for [[clause-type clause] clauses]
         (case clause-type
           :triple (let [{:keys [e a v]} clause]
                     `[~(mapv logic-var-or-blank [e a v])
                       (triple ~'$ ~@(map lvar-ref [e a v]))])
           :rule (let [{:keys [rule-name args] :as rule} clause]
                   `[~(vec args)
                     (~rule-name ~rule-ctx-sym ~@(map lvar-ref args))])
           :pred (let [[expr] clause]
                   [:when (expr-with-lvar-refs expr)])
           :fn (let [[expr binding] clause]
                 [:let [binding (expr-with-lvar-refs expr)]])))
       (reduce into [])))

(defn- rule-leg-name [rule-name idx]
  (symbol (str rule-name "-" idx)))

(defn- rules->clj [rules]
  (let [name->rules (some->> rules
                             (s/assert :db.query/rules)
                             (s/conform :db.query/rules)
                             (group-by (comp :rule-name :rule-head)))]
    (->> (for [[rule-name rule-legs] name->rules
               :let [idx->rule-leg (zipmap (range (count rule-legs)) rule-legs)
                     rule-leg-refs (mapv #(rule-leg-name rule-name %) (keys idx->rule-leg))]]
           (->> (for [[idx {:keys [rule-body]
                            {:keys [bound-vars free-vars]} :rule-head}] idx->rule-leg
                      :let [arg-vars (concat bound-vars free-vars)]]
                  `(~(rule-leg-name rule-name idx) [~rule-ctx-sym ~@arg-vars]
                    (for ~(clauses->clj rule-body)
                      ~(vec arg-vars))))
                (cons `(~rule-name [rule-ctx# & args#]
                        (rule rule-ctx# ~rule-leg-refs args#)))))
         (reduce into []))))

(defn- normalize-query [query]
  (if (vector? query)
    (->> (for [[[k] v] (->> (partition-by keyword? query)
                            (partition-all 2))]
           [k (vec v)])
         (into {}))
    query))

(defn- query->clj [query rules]
  (let [query (->> (normalize-query query)
                   (s/assert :db/query)
                   (s/conform :db/query))
        {:keys [find in where] tuple-keys :keys} query]
    `(fn ~(or in ['$])
       (let [~rule-ctx-sym #{}]
         (letfn ~(rules->clj rules)
           (for ~(clauses->clj where)
             ~(if tuple-keys
                `(zipmap '~tuple-keys ~find)
                find)))))))

(def ^:private memo-compile-query
  (memoize
   (fn [query rules]
     (eval (query->clj query rules)))))

(defn qseq [{:keys [query args]}]
  (let [{:keys [in]} (s/conform :db/query (normalize-query query))
        {rules '%} (zipmap in args)]
    (apply (memo-compile-query query rules) args)))

(defn q [query & inputs]
  (set (qseq {:query query :args inputs})))

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

(defn transact [db tx-ops]
  (reduce
   (fn [db tx-op]
     (s/assert :db/tx-op tx-op)
     (let [[op-type {:keys [e a v] :as tx-op}] (s/conform :db/tx-op tx-op)]
       (case op-type
         :entity (add-entity db tx-op)
         :retract-entity (retract-entity db e)
         :add (add-triple db [e a v])
         :retract (retract-triple db [e a v])
         :cas (let [{:keys [old-v new-v]} tx-op]
                (if (seq (datoms db :eav e a old-v))
                  (reduce transact db [[:db/retract e a old-v]
                                       [:db/add e a new-v]])
                  db))
         :fn (let [{:keys [fn args]} tx-op]
               (reduce transact db (apply fn db args))))))
   db
   tx-ops))

(comment

  (let [db (->> '[{:db/id b1
                   :qgm.box/type :qgm.box.type/base-table
                   :qgm.box.base-table/name inventory}
                  {:db/id b2
                   :qgm.box/type :qgm.box.type/base-table
                   :qgm.box.base-table/name quotations}
                  {:db/id b3
                   :qgm.box/type :qgm.box.type/select
                   :qgm.box.head/distinct? true
                   :qgm.box.head/columns [partno descr suppno]
                   :qgm.box.body/columns [q1.partno q1.descr q2.suppno]
                   :qgm.box.body/distinct :qgm.box.body.distinct/enforce
                   :qgm.box.body/quantifiers #{q1 q2 q4}}
                  {:db/id b4
                   :qgm.box/type :qgm.box.type/select
                   :qgm.box.head/distinct? false
                   :qgm.box.head/columns [price]
                   :qgm.box.body/columns [q3.price]
                   :qgm.box.body/distinct :qgm.box.body.distinct/permit
                   :qgm.box.body/quantifiers #{q3}}

                  {:db/id q1
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno descr]
                   :qgm.quantifier/ranges-over box1}
                  {:db/id q2
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno price]
                   :qgm.quantifier/ranges-over box2}
                  {:db/id q3
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno price]
                   :qgm.quantifier/ranges-over box2}
                  {:db/id q4
                   :qgm.quantifier/type :qgm.quantifier.type/all
                   :qgm.quantifier/columns [price]
                   :qgm.quantifier/ranges-over box4}

                  {:db/id p1
                   :qgm.predicate/expression (= q1.descr "engine")
                   :qgm.predicate/quantifiers #{q1}}
                  {:db/id p2
                   :qgm.predicate/expression (= q1.partno q2.partno)
                   :qgm.predicate/quantifiers #{q1 q2}}
                  {:db/id p3
                   :qgm.predicate/expression (<= q2.partno q4.partno)
                   :qgm.predicate/quantifiers #{q2 q4}}
                  {:db/id p4
                   :qgm.predicate/expression (= q2.partno q3.partno)
                   :qgm.predicate/quantifiers #{q2 q3}}]
                (map #(s/assert :qgm/node %))
                (transact {}))]

    (entity db 'b3)
    (datoms db :aev :qgm.box.body/quantifiers 'b3)

    (q '[:find ?b ?q ?t
         :in $ ?b
         :where
         [?b :qgm.box.body/quantifiers ?q]
         [?q :qgm.quantifier/type ?t]]
       db 'b3)))

(comment

  (let [db (transact {}
                     '[{:db/id :john :parent :douglas}
                       {:db/id :bob :parent :john}
                       {:db/id :ebbon :parent :bob}])]
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

  (let [db (transact {}
                     '[{:db/id :a :edge :b}
                       {:db/id :b :edge :c}
                       {:db/id :c :edge :d}
                       {:db/id :d :edge :a}])]
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
             (path ?z ?y)]]))))
