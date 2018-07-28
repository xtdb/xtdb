(ns crux.query
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.walk :as w]
            [com.stuartsierra.dependency :as dep]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv-store :as ks]
            [crux.db :as db]))

(defn- logic-var? [x]
  (symbol? x))

(def ^:private literal? (complement logic-var?))
(def ^:private db-ident? keyword?)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next)
         spec))

(def ^:private built-ins '#{and == !=})

(s/def ::bgp (s/and vector? (s/cat :e (some-fn logic-var? db-ident?)
                                   :a db-ident?
                                   :v (s/? any?))))

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(or (some-> % resolve var-get) %))
                        (some-fn fn? logic-var?)))
(s/def ::pred (s/and vector? (s/cat :pred (s/and list?
                                                 (s/cat :pred-fn ::pred-fn
                                                        :args (s/* any?)))
                                    :return (s/? logic-var?))))

(s/def ::rule (s/and list? (s/cat :name (s/and symbol? (complement built-ins))
                                  :args (s/+ any?))))

(s/def ::range-op '#{< <= >= >})
(s/def ::range (s/tuple (s/and list?
                               (s/or :sym-val (s/cat :op ::range-op
                                                     :sym logic-var?
                                                     :val literal?)
                                     :val-sym (s/cat :op ::range-op
                                                     :val literal?
                                                     :sym logic-var?)))))

(s/def ::unify (s/tuple (s/and list?
                               (s/cat :op '#{== !=}
                                      :args (s/+ any?)))))

(s/def ::args-list (s/coll-of logic-var? :kind vector? :min-count 1))

(s/def ::not (expression-spec 'not (s/+ ::term)))
(s/def ::not-join (expression-spec 'not-join (s/cat :args ::args-list
                                                    :body (s/+ ::term))))

(s/def ::and (expression-spec 'and (s/+ ::term)))
(s/def ::or-body (s/+ (s/or :term ::term
                            :and ::and)))
(s/def ::or (expression-spec 'or ::or-body))
(s/def ::or-join (expression-spec 'or-join (s/cat :args ::args-list
                                                  :body ::or-body)))

(s/def ::term (s/or :bgp ::bgp
                    :not ::not
                    :not-join ::not-join
                    :or ::or
                    :or-join ::or-join
                    :range ::range
                    :unify ::unify
                    :rule ::rule
                    :pred ::pred))

(s/def ::find ::args-list)
(s/def ::where (s/coll-of ::term :kind vector? :min-count 1))

(s/def ::arg-tuple (s/map-of (some-fn logic-var? keyword?) any?))
(s/def ::args (s/coll-of ::arg-tuple :kind vector?))

(s/def ::rule-head (s/and list?
                          (s/cat :name (s/and symbol? (complement built-ins))
                                 :bound-args (s/? ::args-list)
                                 :args (s/* logic-var?))))
(s/def ::rule-definition (s/and vector?
                                (s/cat :head ::rule-head
                                       :body (s/+ ::term))))
(s/def ::rules (s/coll-of ::rule-definition :kind vector? :min-count 1))

(s/def ::query (s/keys :req-un [::find ::where] :opt-un [::args ::rules]))

;; NOTE: :min-count generates boxed math warnings, so this goes below
;; the spec.
(set! *unchecked-math* :warn-on-boxed)

(defn- cartesian-product [[x & xs]]
  (when (seq x)
    (for [a x
          bs (or (cartesian-product xs) [[]])]
      (cons a bs))))

(defn- blank-var? [v]
  (when (logic-var? v)
    (re-find #"^_\d*$" (name v))))

(defn- normalize-bgp-clause [{:keys [e a v] :as clause}]
  (cond-> clause
    (or (blank-var? v)
        (nil? v))
    (assoc :v (gensym "_"))
    (blank-var? e)
    (assoc :e (gensym "_"))
    (nil? a)
    (assoc :a :crux.db/id)))

(def ^:private pred->built-in-range-pred {< (comp neg? compare)
                                          <= (comp not pos? compare)
                                          > (comp pos? compare)
                                          >= (comp not neg? compare)})

(def ^:private range->inverse-range '{< >=
                                      <= >
                                      > <=
                                      >= <})

;; NOTE: This could be optimised if the binary join had a mechanism to
;; propagate the value the first occurrence to the second. This would
;; avoid having to scan the entire second var to find the value we
;; already know we're looking for. This could be implemented as a
;; relation that gets updated once we know the first value. In the
;; more generic case, unification constraints could propagate
;; knowledge of the first bound value from one join to another.
(defn- rewrite-self-join-bgp-clause [{:keys [e v] :as bgp}]
  (let [v-var (gensym v)]
    {:bgp [(assoc bgp :v v-var)]
     :unify [{:op '== :args [v-var e]}]}))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         (if (= :bgp type)
           (let [{:keys [e v] :as clause} (normalize-bgp-clause clause)]
             (if (and (logic-var? e) (= e v))
               (rewrite-self-join-bgp-clause clause)
               {:bgp [clause]}))
           {type [(case type
                    :pred (let [{:keys [pred]} clause
                                {:keys [pred-fn args]} pred]
                            (if-let [range-pred (and (= 2 (count args))
                                                     (every? logic-var? args)
                                                     (get pred->built-in-range-pred pred-fn))]
                              (assoc-in clause [:pred :pred-fn] range-pred)
                              clause))
                    :range (let [[type clause] (first clause)]
                             (if (= :val-sym type)
                               (update clause :op range->inverse-range)
                               clause))
                    :unify (first clause)
                    clause)]}))
       (apply merge-with into)))

(defn- collect-vars [{bgp-clauses :bgp
                      unify-clauses :unify
                      not-clauses :not
                      not-join-clauses :not-join
                      or-clauses :or
                      or-join-clauses :or-join
                      pred-clauses :pred
                      range-clauses :range
                      rule-clauses :rule}]
  (let [or-vars (->> (for [or-clause or-clauses
                           [type sub-clauses] or-clause]
                       (collect-vars (normalize-clauses (case type
                                                          :term [sub-clauses]
                                                          :and sub-clauses))))
                     (apply merge-with set/union))
        not-join-vars (set (for [not-join-clause not-join-clauses
                                 arg (:args not-join-clause)]
                             arg))
        not-vars (->> (for [not-clause not-clauses]
                        (collect-vars (normalize-clauses not-clause)))
                      (apply merge-with set/union))
        or-join-vars (set (for [or-join-clause or-join-clauses
                                arg (:args or-join-clause)]
                            arg))]
    {:e-vars (set (for [{:keys [e]} bgp-clauses
                        :when (logic-var? e)]
                    e))
     :v-vars (set (for [{:keys [v]} bgp-clauses
                        :when (logic-var? v)]
                    v))
     :unification-vars (set (for [{:keys [args]} unify-clauses
                                  arg args
                                  :when (logic-var? arg)]
                              arg))
     :not-vars (->> (vals not-vars)
                    (reduce into not-join-vars))
     :pred-vars (set (for [{:keys [pred return]} pred-clauses
                           arg (cons return (cons (:pred-fn pred) (:args pred)))
                           :when (logic-var? arg)]
                       arg))
     :pred-return-vars (set (for [{:keys [pred return]} pred-clauses
                                  :when (logic-var? return)]
                              return))
     :range-vars (set (for [{:keys [sym]} range-clauses]
                        sym))
     :or-vars (apply set/union (vals or-vars))
     :rule-vars (set/union (set (for [{:keys [args]} rule-clauses
                                      arg args
                                      :when (logic-var? arg)]
                                  arg))
                           or-join-vars)}))

(defn- build-v-var-range-constraints [e-vars range-clauses]
  (let [v-var->range-clauses (->> (for [{:keys [sym] :as clause} range-clauses]
                                    (if (contains? e-vars sym)
                                      (throw (IllegalArgumentException.
                                              (str "Cannot add range constraints on entity variable: "
                                                   (pr-str clause))))
                                      clause))
                                  (group-by :sym))]
    (->> (for [[v-var clauses] v-var->range-clauses]
           [v-var (->> (for [{:keys [op val]} clauses]
                         (case op
                           < #(doc/new-less-than-virtual-index % val)
                           <= #(doc/new-less-than-equal-virtual-index % val)
                           > #(doc/new-greater-than-virtual-index % val)
                           >= #(doc/new-greater-than-equal-virtual-index % val)))
                       (apply comp))])
         (into {}))))

(defn- arg-for-var [arg var]
  (or (get arg (symbol (name var)))
      (get arg (keyword (name var)))))

(defn- update-binary-index [snapshot {:keys [business-time transact-time]} binary-idx vars-in-join-order v-var->range-constriants]
  (let [{:keys [clause names]} (meta binary-idx)
        {:keys [e a v]} clause
        order (filter (set (vals names)) vars-in-join-order)
        v-range-constraints (get v-var->range-constriants v)
        entity-as-of-idx (doc/new-entity-as-of-index snapshot business-time transact-time)]
    (if (= (:v names) (first order))
      (let [v-doc-idx (doc/new-doc-attribute-value-entity-value-index (ks/new-iterator snapshot) a)
            e-idx (doc/new-doc-attribute-value-entity-entity-index (ks/new-iterator snapshot) a v-doc-idx entity-as-of-idx)]
        (log/debug :join-order :ave (pr-str v) e (pr-str clause))
        (doc/update-binary-join-order! binary-idx (doc/wrap-with-range-constraints v-doc-idx v-range-constraints) e-idx))
      (let [e-doc-idx (doc/new-doc-attribute-entity-value-entity-index (ks/new-iterator snapshot) a entity-as-of-idx)
            v-idx (-> (doc/new-doc-attribute-entity-value-value-index (ks/new-iterator snapshot) a e-doc-idx)
                      (doc/wrap-with-range-constraints v-range-constraints))]
        (log/debug :join-order :aev e (pr-str v) (pr-str clause))
        (doc/update-binary-join-order! binary-idx e-doc-idx v-idx)))))

(defn- bgp-joins [snapshot {:keys [object-store business-time transact-time] :as db} bgp-clauses var->joins non-leaf-vars]
  (let [v->clauses (group-by :v bgp-clauses)]
    (->> bgp-clauses
         (reduce
          (fn [[deps var->joins] {:keys [e a v] :as clause}]
            (let [e-var e
                  v-var (if (logic-var? v)
                          v
                          (gensym (str "literal_" v "_")))
                  binary-idx (with-meta (doc/new-binary-join-virtual-index) {:clause clause
                                                                             :names {:e e-var
                                                                                     :v v-var}})
                  indexes {v-var [(assoc binary-idx :name e-var)]
                           e-var [(assoc binary-idx :name e-var)]}
                  indexes (if (literal? e)
                            (merge-with into indexes {e-var [(doc/new-relation-virtual-index e-var [[e]] 1)]})
                            indexes)
                  indexes (if (literal? v)
                            (merge-with into indexes {v-var [(doc/new-relation-virtual-index v-var [[v]] 1)]})
                            indexes)
                  v-is-leaf? (and (logic-var? v)
                                  (= 1 (count (get v->clauses v)))
                                  (not (contains? non-leaf-vars v)))]
              [(if (= e v)
                 deps
                 (cond-> deps
                   (and (logic-var? v)
                        (literal? e))
                   (conj [[e-var] [v-var]])
                   (and (literal? v)
                        (logic-var? e))
                   (conj [[v-var] [e-var]])
                   ;; TODO: This is to default join order to ave as it
                   ;; used to be as some things break without
                   ;; it. Those breakages likely have other root
                   ;; causes that should be fixed eventually.
                   (and (logic-var? v)
                        (logic-var? e)
                        (not v-is-leaf?))
                   (conj [[v-var] [e-var]])
                   v-is-leaf?
                   (conj [[e-var] [v-var]])))
               (merge-with into var->joins indexes)]))
          [[] var->joins]))))

(defn- arg-vars [args]
  (let [ks (keys (first args))]
    (doseq [m args]
      (when-not (every? #(contains? m %) ks)
        (throw (IllegalArgumentException.
                (str "Argument maps need to contain the same keys as first map: " ks " " (keys m))))))
    (set (for [k ks]
           (symbol (name k))))))

(defn- arg-joins [snapshot {:keys [business-time transact-time] :as db} args e-vars v-var->range-constriants var->joins]
  (let [arg-vars (arg-vars args)
        relation (doc/new-relation-virtual-index (gensym "args")
                                                 []
                                                 (count arg-vars))]
    [relation
     (->> arg-vars
          (reduce
           (fn [var->joins arg-var]
             (->> {arg-var
                   [(assoc relation :name (symbol "crux.query.value" (name arg-var)))]}
                  (merge-with into var->joins)))
           var->joins))]))

(defn- pred-joins [pred-clauses v-var->range-constriants var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause+relations var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [relation (doc/new-relation-virtual-index (gensym "pred-return")
                                                           []
                                                           1
                                                           [(get v-var->range-constriants return)])]
              [(conj pred-clause+relations [pred-clause relation])
               (->> {return
                     [(assoc relation :name (symbol "crux.query.value" (name return)))]}
                    (merge-with into var->joins))])
            [(conj pred-clause+relations [pred-clause])
             var->joins]))
        [[] var->joins])))

(declare build-sub-query)

(defn- or-joins [snapshot db rules or-type or-clauses var->joins known-vars]
  (->> or-clauses
       (reduce
        (fn [[or-clause+relation+or-branches known-vars var->joins] clause]
          (let [or-join? (= :or-join or-type)
                or-branches (for [[type sub-clauses] (case or-type
                                                       :or clause
                                                       :or-join (:body clause))
                                  :let [where (case type
                                                :term [sub-clauses]
                                                :and sub-clauses)
                                        body-vars (->> (collect-vars (normalize-clauses where))
                                                       (vals)
                                                       (reduce into #{}))
                                        or-vars (if or-join?
                                                  (set (:args clause))
                                                  body-vars)
                                        free-vars (set/difference or-vars known-vars)]]
                              (do (when or-join?
                                    (doseq [var or-vars
                                            :when (not (contains? body-vars var))]
                                      (throw (IllegalArgumentException.
                                              (str "Or join variable never used: " var " " (pr-str clause))))))
                                  {:or-vars or-vars
                                   :free-vars free-vars
                                   :bound-vars (set/difference or-vars free-vars)
                                   :where where}))
                free-vars (:free-vars (first or-branches))
                relation (when (seq free-vars)
                           (doc/new-relation-virtual-index (gensym "or-free-vars")
                                                           []
                                                           (count free-vars)))]
            (when (not (apply = (map :or-vars or-branches)))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variables: " (pr-str clause)))))
            [(conj or-clause+relation+or-branches [clause relation or-branches])
             (into known-vars free-vars)
             (apply merge-with into var->joins (for [v free-vars]
                                                 {v [(assoc relation :name (symbol "crux.query.value" (name v)))]}))]))
        [[] known-vars var->joins])))

(defn- build-var-bindings [var->attr v-var->e e->v-var var->values-result-index join-depth vars]
  (->> (for [var vars
             :let [e (get v-var->e var var)
                   join-depth (or (max (long (get var->values-result-index e -1))
                                       (long (get var->values-result-index (get e->v-var e) -1)))
                                  (dec (long join-depth)))
                   result-index (get var->values-result-index var)]]
         [var {:e-var e
               :var var
               :attr (get var->attr var)
               :result-index result-index
               :join-depth join-depth
               :result-name e
               :type :entity}])
       (into {})))

(defn- build-arg-var-bindings [var->values-result-index arg-vars]
  (->> (for [var arg-vars
             :let [result-index (get var->values-result-index var)]]
         [var {:var var
               :result-name (symbol "crux.query.value" (name var))
               :result-index result-index
               :join-depth result-index
               :type :arg}])
       (into {})))

(defn- build-pred-return-var-bindings [var->values-result-index pred-clauses]
  (->> (for [{:keys [return]} pred-clauses
             :when return
             :let [result-index (get var->values-result-index return)]]
         [return {:var return
                  :result-name (symbol "crux.query.value" (name return))
                  :result-index result-index
                  :join-depth result-index
                  :type :pred}])
       (into {})))

(defn- build-or-free-var-bindings [var->values-result-index or-clause+relation+or-branches]
  (->> (for [[_ _ or-branches] or-clause+relation+or-branches
             var (:free-vars (first or-branches))
             :let [result-index (get var->values-result-index var)]]
         [var {:var var
               :result-name (symbol "crux.query.value" (name var))
               :result-index result-index
               :join-depth result-index
               :type :or}])
       (into {})))

(defn- bound-result->join-result [{:keys [result-name value? type entity value] :as result}]
  (if value?
    {result-name #{value}}
    {result-name #{entity}}))

(defn- bound-results-for-var [object-store var->bindings join-keys join-results var]
  (let [{:keys [e-var var attr result-index result-name type]} (get var->bindings var)]
    (if (= "crux.query.value" (namespace result-name))
      (for [value (get join-results result-name)]
        {:value value
         :result-name result-name
         :type type
         :var var
         :value? true})
      (when-let [entities (not-empty (get join-results e-var))]
        (let [content-hashes (map :content-hash entities)
              content-hash->doc (db/get-objects object-store content-hashes)
              value-bytes (get join-keys result-index)]
          (for [[entity doc] (map vector entities (map content-hash->doc content-hashes))
                :let [values (doc/normalize-value (get doc attr))]
                value values
                :when (or (nil? value-bytes)
                          (= (count values) 1)
                          (bu/bytes=? value-bytes (idx/value->bytes value)))]
            {:value value
             :e-var e-var
             :v-var var
             :attr attr
             :doc doc
             :entity entity
             :result-name result-name
             :type type
             :var var
             :value? false}))))))

(defn- calculate-constraint-join-depth [var->bindings vars]
  (->> (for [var vars]
         (get-in var->bindings [var :join-depth] -1))
       (apply max -1)
       (long)
       (inc)))

(defn- build-pred-constraints [object-store pred-clause+relations var->bindings]
  (for [[{:keys [pred return] :as clause} relation] pred-clause+relations
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)]]
    (do (doseq [var pred-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Predicate refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn pred-constraint [join-keys join-results]
          (if (= (count join-keys) pred-join-depth)
            (let [bound-result-args-tuples (if (empty? args)
                                             [{}]
                                             (cartesian-product
                                              (for [arg args]
                                                (if (logic-var? arg)
                                                  (bound-results-for-var object-store var->bindings join-keys join-results arg)
                                                  [{:value arg
                                                    :literal-arg? true}]))))
                  pred-fns (if (logic-var? pred-fn)
                             (mapv :value (bound-results-for-var object-store var->bindings join-keys join-results pred-fn))
                             [pred-fn])
                  pred-result+result-maps
                  (for [bound-result-args-tuple bound-result-args-tuples
                        pred-fn pred-fns
                        :let [pred-result (apply pred-fn (mapv :value bound-result-args-tuple))]
                        :when pred-result]
                    [pred-result
                     (->> (for [{:keys [literal-arg?] :as result} bound-result-args-tuple
                                :when (not literal-arg?)]
                            (bound-result->join-result result))
                          (apply merge-with into))])]
              (when return
                (->> (for [[pred-result] pred-result+result-maps]
                       [pred-result])
                     (distinct)
                     (vec)
                     (doc/update-relation-virtual-index! relation)))
              (when-let [join-results (some->> (map second pred-result+result-maps)
                                               (apply merge-with into)
                                               (merge join-results))]
                join-results))
            join-results)))))

(defn- single-e-var-bgp? [vars where]
  (and (= 1 (count where))
       (let [[[type {:keys [e v]}]] where]
         (and (= :bgp type)
              (contains? vars e)
              (logic-var? e)
              (literal? v)))))

;; TODO: For or (but not or-join) it might be possible to embed the
;; entire or expression into the parent join via either OrVirtualIndex
;; (though as all joins now are binary they have variable order
;; dependency so this might work easily) or NAryOrVirtualIndex for the
;; generic case. As constants are represented by relations, which
;; introduce new vars which would have to be lifted up to the parent
;; join as all or branches need to have the same variables. Another
;; problem when embedding joins are the sub joins constraints which
;; need to fire at the right level, but they won't currently know how
;; to translate their local join depth to the join depth in the
;; parent, which is what will be used when walking the tree. Due to
;; the way or-join (and rules) work, they likely have to stay as sub
;; queries. Recursive rules always have to be sub queries.
(defn- or-single-e-var-bgp-fast-path [snapshot {:keys [object-store business-time transact-time] :as db} where args]
  (let [[[_ {:keys [e a v] :as clause}]] where
        entities (doc/entities-at snapshot (mapv e args) business-time transact-time)
        content-hash->doc (db/get-objects object-store (map :content-hash entities))
        result (set (for [{:keys [content-hash] :as entity} entities
                          :let [doc (get content-hash->doc content-hash)]
                          :when (contains? (set (doc/normalize-value (get doc a))) v)]
                      entity))]
    (when (seq result)
      [[[]
        {e result}]])))

(def ^:private ^:dynamic *recursion-table* {})

;; TODO: This tabling mechanism attempts at avoiding infinite
;; recursion, but does not actually cache anything. Short-circuits
;; identical sub trees. Passes tests, unsure if this really works in
;; the general case. Depends on the eager expansion of rules for some
;; cases to pass. One alternative is maybe to try to cache the
;; sequence and reuse it, somehow detecting if it loops.
(defn- build-or-constraints [snapshot {:keys [object-store] :as db} rule-name->rules or-clause+relation+or-branches
                             var->bindings vars-in-join-order v-var->range-constriants]
  (for [[clause relation [{:keys [free-vars bound-vars]} :as or-branches]] or-clause+relation+or-branches
        :let [or-join-depth (calculate-constraint-join-depth var->bindings bound-vars)
              free-vars-in-join-order (filter (set free-vars) vars-in-join-order)
              {:keys [rule-name]} (meta clause)]]
    (do (doseq [var bound-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Or refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn or-constraint [join-keys join-results]
          (if (= (count join-keys) or-join-depth)
            (let [args (vec (for [bound-result-tuple (cartesian-product
                                                      (for [var bound-vars]
                                                        (bound-results-for-var object-store var->bindings join-keys join-results var)))]
                              (zipmap bound-vars (map :value bound-result-tuple))))
                  free-results+bound-results
                  (let [free-results+bound-results
                        (->> (for [[branch-index {:keys [where] :as or-branch}] (map-indexed vector or-branches)
                                   :let [cache-key (when rule-name
                                                     [rule-name branch-index (count free-vars) (set (mapv vals args))])]]
                               (with-open [snapshot (doc/new-cached-snapshot snapshot false)]
                                 (if (single-e-var-bgp? bound-vars where)
                                   (or-single-e-var-bgp-fast-path snapshot db where args)
                                   (or (get *recursion-table* cache-key)
                                       (binding [*recursion-table* (if cache-key
                                                                     (assoc *recursion-table* cache-key [])
                                                                     *recursion-table*)]
                                         (let [{:keys [n-ary-join
                                                       var->bindings]} (build-sub-query snapshot db where args rule-name->rules)]
                                           (vec (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join)]
                                                  [(vec (for [bound-result-tuple (cartesian-product
                                                                                  (for [var free-vars-in-join-order]
                                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))]
                                                          (mapv :value bound-result-tuple)))
                                                   (->> (for [bound-result-tuple (cartesian-product
                                                                                  (for [var bound-vars]
                                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                                              result bound-result-tuple]
                                                          (bound-result->join-result result))
                                                        (apply merge-with into))]))))))))
                             (reduce into []))]
                    free-results+bound-results)
                  free-results (->> (for [[free-results] free-results+bound-results
                                          free-result free-results]
                                      free-result)
                                    (distinct)
                                    (vec))
                  bound-results (->> (for [[_ bound-result] free-results+bound-results]
                                       bound-result)
                                     (apply merge-with into))]
              (when (seq free-results)
                (doc/update-relation-virtual-index! relation free-results (map v-var->range-constriants free-vars-in-join-order)))

              (if (empty? bound-vars)
                (when (seq free-results)
                  join-results)
                (when (seq bound-results)
                  (merge join-results bound-results))))
            join-results)))))

;; TODO: Unification could be improved by using dynamic relations
;; propagating knowledge from the first var to the next. See comment
;; about this at rewrite-self-join-bgp-clause. Currently unification
;; has to scan the values and check them as they get bound and doesn't
;; fully carry its weight compared to normal predicates.
(defn- build-unification-constraints [snapshot {:keys [object-store] :as db} unify-clauses var->bindings]
  (for [{:keys [op args]
         :as clause} unify-clauses
        :let [unify-vars (filter logic-var? args)
              unification-join-depth (calculate-constraint-join-depth var->bindings unify-vars)]]
    (do (doseq [arg args
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Unification refers to unknown variable: "
                       arg " " (pr-str clause)))))
        (fn unification-constraint [join-keys join-results]
          (if (= (count join-keys) unification-join-depth)
            (let [values (for [arg args]
                           (if (logic-var? arg)
                             (let [{:keys [result-index]} (get var->bindings arg)]
                               (->> (get join-keys result-index)
                                    (sorted-set-by bu/bytes-comparator)))
                             (->> (map idx/value->bytes (doc/normalize-value arg))
                                  (into (sorted-set-by bu/bytes-comparator)))))]
              (when (case op
                      == (boolean (not-empty (apply set/intersection values)))
                      != (empty? (apply set/intersection values)))
                join-results))
            join-results)))))

(defn- build-not-constraints [snapshot {:keys [object-store] :as db} rule-name->rules not-type not-clauses var->bindings]
  (for [not-clause not-clauses
        :let [[not-vars not-clause] (case not-type
                                      :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                            not-clause]
                                      :not-join [(:args not-clause)
                                                 (:body not-clause)])
              not-vars (remove blank-var? not-vars)
              not-join-depth (calculate-constraint-join-depth var->bindings not-vars)]]
    (do (doseq [arg not-vars
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Not refers to unknown variable: "
                       arg " " (pr-str not-clause)))))
        (fn not-constraint [join-keys join-results]
          (if (= (count join-keys) not-join-depth)
            (with-open [snapshot (doc/new-cached-snapshot snapshot false)]
              (let [args (vec (for [bound-result-tuple (cartesian-product
                                                        (for [var not-vars]
                                                          (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                    :let [args-tuple (zipmap not-vars (map :value bound-result-tuple))]]
                                (with-meta args-tuple {:bound-result-tuple bound-result-tuple})))
                    {:keys [n-ary-join
                            var->bindings]} (build-sub-query snapshot db not-clause args rule-name->rules)
                    args-to-remove (set (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join)
                                              bound-result-tuple (cartesian-product
                                                                  (for [var not-vars]
                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))]
                                          (zipmap not-vars (map :value bound-result-tuple))))
                    args-to-keep (remove args-to-remove args)]
                (when-let [join-results (some->> (for [args args-to-keep
                                                       :let [{:keys [bound-result-tuple]} (meta args)]
                                                       result bound-result-tuple]
                                                   (bound-result->join-result result))
                                                 (apply merge-with into)
                                                 (merge join-results))]
                  join-results)))
            join-results)))))

(defn- constrain-join-result-by-constraints [constraints join-keys join-results]
  (reduce
   (fn [results constraint]
     (when results
       (constraint join-keys results)))
   join-results
   constraints))

;; TODO: This is potentially simplistic. Needs revisiting, should be
;; possible to clean up.
(defn- calculate-join-order [pred-clauses or-clause+relation+or-branches var->joins arg-vars join-deps]
  (let [pred-deps (for [{:keys [pred return] :as pred-clause} pred-clauses]
                    [(filter logic-var? (:args pred))
                     (if return
                       [return]
                       [])])
        or-deps (for [[_ _ [{:keys [free-vars bound-vars]}]] or-clause+relation+or-branches]
                  [bound-vars free-vars])
        g (dep/graph)
        g (->> (keys var->joins)
               (reduce
                (fn [g v]
                  (dep/depend g v ::root))
                g))
        g (->> (concat pred-deps or-deps join-deps)
               (reduce
                (fn [g [dependencies dependents]]
                  (->> dependencies
                       (reduce
                        (fn [g dependency]
                          (->> dependents
                               (reduce
                                (fn [g dependent]
                                  (dep/depend g dependent dependency))
                                g)))
                        g)))
                g))
        join-order (dep/topo-sort g)]
    (vec (filter var->joins join-order))))

(defn- expand-rules [where rule-name->rules recursion-cache]
  (->> (for [[type clause :as sub-clause] where]
         (if (= :rule type)
           (let [rule-name (:name clause)
                 rules (get rule-name->rules rule-name)]
             (when-not rules
               (throw (IllegalArgumentException.
                       (str "Unknown rule: " (pr-str sub-clause)))))
             (let [rule-args+body (for [{:keys [head body]} rules]
                                    [(vec (concat (:bound-args head)
                                                  (:args head)))
                                     body])
                   [arity :as arities] (->> rule-args+body
                                            (map (comp count first))
                                            (distinct))]
               (when-not (= 1 (count arities))
                 (throw (IllegalArgumentException. (str "Rule definitions require same arity: " (pr-str rules)))))
               (when-not (= arity (count (:args clause)))
                 (throw (IllegalArgumentException.
                         (str "Rule invocation has wrong arity, expected: " arity " " (pr-str sub-clause)))))
               ;; TODO: the caches and expansion here needs
               ;; revisiting.
               (let [expanded-rules (for [[branch-index [rule-args body]] (map-indexed vector rule-args+body)
                                          :let [rule-arg->query-arg (zipmap rule-args (:args clause))
                                                body-vars (->> (collect-vars (normalize-clauses body))
                                                               (vals)
                                                               (reduce into #{}))
                                                body-var->hidden-var (zipmap body-vars
                                                                             (map gensym body-vars))]]
                                      (w/postwalk-replace (merge body-var->hidden-var rule-arg->query-arg) body))
                     cache-key [:seen-rules rule-name]
                     ;; TODO: Understand this, does this really work
                     ;; in the general case?
                     expanded-rules (if (zero? (long (get-in recursion-cache cache-key 0)))
                                      (for [expanded-rule expanded-rules
                                            :let [expanded-rule (expand-rules expanded-rule rule-name->rules
                                                                              (update-in recursion-cache cache-key (fnil inc 0)))]
                                            :when (seq expanded-rule)]
                                        expanded-rule)
                                      expanded-rules)]
                 (if (= 1 (count expanded-rules))
                   (first expanded-rules)
                   (when (seq expanded-rules)
                     [[:or-join
                       (with-meta
                         {:args (vec (filter logic-var? (:args clause)))
                          :body (vec (for [expanded-rule expanded-rules]
                                       [:and expanded-rule]))}
                         {:rule-name rule-name})]])))))
           [sub-clause]))
       (reduce into [])))

(defn- build-sub-query [snapshot {:keys [kv object-store business-time transact-time] :as db} where args rule-name->rules]
  (let [where (expand-rules where rule-name->rules {})
        {bgp-clauses :bgp
         range-clauses :range
         pred-clauses :pred
         unify-clauses :unify
         not-clauses :not
         not-join-clauses :not-join
         or-clauses :or
         or-join-clauses :or-join
         :as type->clauses} (normalize-clauses where)
        {:keys [e-vars
                v-vars
                pred-return-vars]} (collect-vars type->clauses)
        v-var->e (->> (for [{:keys [e v] :as clause} bgp-clauses
                            :when (logic-var? v)]
                        [v e])
                      (into {}))
        e->v-var (set/map-invert v-var->e)
        arg-vars (arg-vars args)
        var->joins {}
        v-var->range-constriants (build-v-var-range-constraints e-vars range-clauses)
        v-range-vars (set (keys v-var->range-constriants))
        non-leaf-vars (set/union e-vars arg-vars v-range-vars)
        [join-deps var->joins] (bgp-joins snapshot
                                          db
                                          bgp-clauses
                                          var->joins
                                          non-leaf-vars)
        [args-relation var->joins] (arg-joins snapshot
                                              db
                                              args
                                              e-vars
                                              v-var->range-constriants
                                              var->joins)
        [pred-clause+relations var->joins] (pred-joins pred-clauses v-var->range-constriants var->joins)
        known-vars (set/union e-vars v-vars pred-return-vars arg-vars)
        [or-clause+relation+or-branches known-vars var->joins] (or-joins snapshot
                                                                         db
                                                                         rule-name->rules
                                                                         :or
                                                                         or-clauses
                                                                         var->joins
                                                                         known-vars)
        [or-join-clause+relation+or-branches known-vars var->joins] (or-joins snapshot
                                                                              db
                                                                              rule-name->rules
                                                                              :or-join
                                                                              or-join-clauses
                                                                              var->joins
                                                                              known-vars)
        or-clause+relation+or-branches (concat or-clause+relation+or-branches
                                               or-join-clause+relation+or-branches)
        v-var->attr (->> (for [{:keys [e a v]} bgp-clauses
                               :when (and (logic-var? v)
                                          (= e (get v-var->e v)))]
                           [v a])
                         (into {}))
        e-var->attr (zipmap e-vars (repeat :crux.db/id))
        var->attr (merge e-var->attr v-var->attr)
        join-depth (count var->joins)
        vars-in-join-order (calculate-join-order pred-clauses or-clause+relation+or-branches var->joins arg-vars join-deps)
        var->values-result-index (zipmap vars-in-join-order (range))
        var->bindings (merge (build-or-free-var-bindings var->values-result-index or-clause+relation+or-branches)
                             (build-pred-return-var-bindings var->values-result-index pred-clauses)
                             (build-arg-var-bindings var->values-result-index arg-vars)
                             (build-var-bindings var->attr
                                                 v-var->e
                                                 e->v-var
                                                 var->values-result-index
                                                 join-depth
                                                 (keys var->attr)))
        unification-constraints (build-unification-constraints snapshot db unify-clauses var->bindings)
        not-constraints (build-not-constraints snapshot db rule-name->rules :not not-clauses var->bindings)
        not-join-constraints (build-not-constraints snapshot db rule-name->rules :not-join not-join-clauses var->bindings)
        pred-constraints (build-pred-constraints object-store pred-clause+relations var->bindings)
        or-constraints (build-or-constraints snapshot db rule-name->rules or-clause+relation+or-branches
                                             var->bindings vars-in-join-order v-var->range-constriants)
        all-constraints (concat unification-constraints
                                pred-constraints
                                not-constraints
                                not-join-constraints
                                or-constraints)
        constrain-result-fn (fn [join-keys join-results]
                              (constrain-join-result-by-constraints all-constraints join-keys join-results))
        joins (map var->joins vars-in-join-order)
        arg-vars-in-join-order (filter (set arg-vars) vars-in-join-order)
        binary-join-indexes (set (for [join joins
                                       idx join
                                       :when (instance? crux.doc.BinaryJoinLayeredVirtualIndex idx)]
                                   (dissoc idx :name)))]
    (doseq [idx binary-join-indexes]
      (update-binary-index snapshot db idx vars-in-join-order v-var->range-constriants))
    (when (seq args)
      (doc/update-relation-virtual-index! args-relation
                                          (vec (for [arg args]
                                                 (mapv #(arg-for-var arg %) arg-vars-in-join-order)))
                                          (mapv v-var->range-constriants arg-vars-in-join-order)))
    (log/debug :where (pr-str where))
    (log/debug :vars-in-join-order vars-in-join-order)
    (log/debug :var->bindings (pr-str var->bindings))
    (constrain-result-fn [] [])
    {:n-ary-join (-> (mapv doc/new-unary-join-virtual-index joins)
                     (doc/new-n-ary-join-layered-virtual-index)
                     (doc/new-n-ary-constraining-layered-virtual-index constrain-result-fn))
     :var->bindings var->bindings}))

(defn q
  ([{:keys [kv] :as db} q]
   (let [start-time (System/currentTimeMillis)]
     (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot kv) true)]
       (let [result (set (crux.query/q snapshot db q))]
         (log/debug :query-time-ms (- (System/currentTimeMillis) start-time))
         (log/debug :query-result-size (count result))
         result))))
  ([snapshot {:keys [object-store] :as db} q]
   (let [{:keys [find where args rules] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (IllegalArgumentException.
               (str "Invalid input: " (s/explain-str :crux.query/query q)))))
     (log/debug :query (pr-str q))
     (let [rule-name->rules (group-by (comp :name :head) rules)
           {:keys [n-ary-join
                   var->bindings]} (build-sub-query snapshot db where args rule-name->rules)]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Find refers to unknown variable: " var))))
       (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join)
             bound-result-tuple (cartesian-product
                                 (for [var find]
                                   (bound-results-for-var object-store var->bindings join-keys join-results var)))]
         (with-meta
           (mapv :value bound-result-tuple)
           (zipmap (map :var bound-result-tuple) bound-result-tuple)))))))

(defrecord QueryDatasource [kv object-store business-time transact-time])

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (doc/read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException.
                (str "Timed out waiting for: " transact-time
                     " index has:" (doc/read-meta kv :crux.tx-log/tx-time))))))))

(defn db
  ([kv]
   (let [business-time (cio/next-monotonic-date)]
     (->QueryDatasource kv
                        (doc/new-cached-object-store kv)
                        business-time
                        business-time)))
  ([kv business-time]
   (->QueryDatasource kv
                      (doc/new-cached-object-store kv)
                      business-time
                      (cio/next-monotonic-date)))
  ([kv business-time transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout)
   (->QueryDatasource kv
                      (doc/new-cached-object-store kv)
                      business-time
                      transact-time)))
