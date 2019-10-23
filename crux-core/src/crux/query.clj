(ns crux.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [com.stuartsierra.dependency :as dep]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.object-store :as os])
  (:import clojure.lang.ExceptionInfo
           crux.api.ICruxDatasource
           crux.codec.EntityTx
           crux.index.BinaryJoinLayeredVirtualIndex
           java.util.Comparator
           java.util.concurrent.TimeoutException
           java.util.function.Supplier
           org.agrona.ExpandableDirectByteBuffer))

(defn- logic-var? [x]
  (symbol? x))

(def ^:private literal? (complement logic-var?))
(def ^:private db-ident? c/valid-id?)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next)
         spec))

(def ^:private built-ins '#{and == !=})

(s/def ::triple (s/and vector? (s/cat :e (some-fn logic-var? db-ident?)
                                      :a db-ident?
                                      :v (s/? (complement nil?)))))

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(or (some->> (if (qualified-symbol? %)
                                                     (requiring-resolve %)
                                                     (ns-resolve 'clojure.core %))
                                                   (var-get))
                                          %))
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
                                      :args (s/+ (complement nil?))))))

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

(s/def ::term (s/or :triple ::triple
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
(s/def ::offset nat-int?)
(s/def ::limit nat-int?)
(s/def ::full-results? boolean?)

(s/def ::order-element (s/and vector?
                              (s/cat :var logic-var? :direction (s/? #{:asc :desc}))))
(s/def ::order-by (s/coll-of ::order-element :kind vector?))

(s/def ::timout nat-int?)

(declare normalize-query)

(s/def ::query (s/and (s/conformer #'normalize-query)
                      (s/keys :req-un [::find ::where] :opt-un [::args ::rules ::offset ::limit ::order-by ::timeout ::full-results?])))

;; NOTE: :min-count generates boxed math warnings, so this goes below
;; the spec.
(set! *unchecked-math* :warn-on-boxed)

(defn- blank-var? [v]
  (when (logic-var? v)
    (re-find #"^_\d*$" (name v))))

(defn- normalize-triple-clause [{:keys [e a v] :as clause}]
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

(defn- rewrite-self-join-triple-clause [{:keys [e v] :as triple}]
  (let [v-var (gensym (str "self-join_" v "_"))]
    {:triple [(with-meta
                (assoc triple :v v-var)
                {:self-join? true})]
     :unify [{:op '== :args [v-var e]}]}))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         (if (= :triple type)
           (let [{:keys [e v] :as clause} (normalize-triple-clause clause)]
             (if (and (logic-var? e) (= e v))
               (rewrite-self-join-triple-clause clause)
               {:triple [clause]}))
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

(defn- collect-vars [{triple-clauses :triple
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
    {:e-vars (set (for [{:keys [e]} triple-clauses
                        :when (logic-var? e)]
                    e))
     :v-vars (set (for [{:keys [v]} triple-clauses
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
           [v-var (->> (for [{:keys [op val]} clauses
                             :let [type-prefix (c/value-buffer-type-id (c/->value-buffer val))]]
                         (case op
                           < #(-> (idx/new-less-than-virtual-index % val)
                                  (idx/new-prefix-equal-virtual-index type-prefix))
                           <= #(-> (idx/new-less-than-equal-virtual-index % val)
                                   (idx/new-prefix-equal-virtual-index type-prefix))
                           > #(-> (idx/new-greater-than-virtual-index % val)
                                  (idx/new-prefix-equal-virtual-index type-prefix))
                           >= #(-> (idx/new-greater-than-equal-virtual-index % val)
                                   (idx/new-prefix-equal-virtual-index type-prefix))))
                       (apply comp))])
         (into {}))))

(defn- arg-for-var [arg var]
  (or (get arg (symbol (name var)))
      (get arg (keyword (name var)))))

(defn- update-binary-index! [snapshot {:keys [entity-as-of-idx]} binary-idx vars-in-join-order v-var->range-constraints]
  (let [{:keys [clause names]} (meta binary-idx)
        {:keys [e a v]} clause
        order (filter (set (vals names)) vars-in-join-order)
        v-range-constraints (get v-var->range-constraints v)]
    (if (= (:v names) (first order))
      (let [v-doc-idx (idx/new-doc-attribute-value-entity-value-index snapshot a)
            e-idx (idx/new-doc-attribute-value-entity-entity-index snapshot a v-doc-idx entity-as-of-idx)]
        (log/debug :join-order :ave (pr-str v) e (pr-str clause))
        (idx/update-binary-join-order! binary-idx (idx/wrap-with-range-constraints v-doc-idx v-range-constraints) e-idx))
      (let [e-doc-idx (idx/new-doc-attribute-entity-value-entity-index snapshot a entity-as-of-idx)
            v-idx (-> (idx/new-doc-attribute-entity-value-value-index snapshot a e-doc-idx)
                      (idx/wrap-with-range-constraints v-range-constraints))]
        (log/debug :join-order :aev e (pr-str v) (pr-str clause))
        (idx/update-binary-join-order! binary-idx e-doc-idx v-idx)))))

(defn- triple-joins [triple-clauses range-clauses var->joins arg-vars stats]
  (let [var->frequency (->> (concat (map :e triple-clauses)
                                    (map :v triple-clauses)
                                    (map :sym range-clauses))
                            (filter logic-var?)
                            (frequencies))
        triple-clauses (sort-by (fn [{:keys [a]}]
                                  (get stats a 0)) triple-clauses)
        literal-clauses (for [{:keys [e v] :as clause} triple-clauses
                              :when (or (literal? e)
                                        (literal? v))]
                          clause)
        literal->var (->> (for [{:keys [e v]} literal-clauses
                                :when (literal? v)]
                            [v (gensym (str "literal_" v "_"))])
                          (into {}))
        literal-join-order (concat (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       (get literal->var v)
                                       e))
                                   (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       e
                                       v)))
        self-join-clauses (filter (comp :self-join? meta) triple-clauses)
        self-join-vars (map :v self-join-clauses)
        join-order (loop [join-order (concat literal-join-order arg-vars self-join-vars)
                          clauses (->> triple-clauses
                                       (remove (set self-join-clauses))
                                       (remove (set literal-clauses)))]
                     (let [join-order-set (set join-order)
                           clause (first (or (seq (for [{:keys [e v] :as clause} clauses
                                                        :when (or (contains? join-order-set e)
                                                                  (contains? join-order-set v))]
                                                    clause))
                                             clauses))]
                       (if-let [{:keys [e a v]} clause]
                         (recur (->> (sort-by var->frequency [v e])
                                     (reverse)
                                     (concat join-order))
                                (remove #{clause} clauses))
                         join-order)))]
    (log/debug :triple-joins-var->frequency var->frequency)
    (log/debug :triple-joins-join-order join-order)
    [(->> join-order
          (distinct)
          (partition 2 1)
          (reduce
           (fn [g [a b]]
             (dep/depend g b a))
           (dep/graph)))
     (->> triple-clauses
          (reduce
           (fn [var->joins {:keys [e a v] :as clause}]
             (let [e-var e
                   v-var (if (logic-var? v)
                           v
                           (get literal->var v))
                   join {:id (gensym "triple")
                         :name e-var
                         :idx-fn #(-> (idx/new-binary-join-virtual-index)
                                      (with-meta {:clause clause
                                                  :names {:e e-var
                                                          :v v-var}}))}
                   var->joins (merge-with into var->joins {v-var [join]
                                                           e-var [join]})
                   var->joins (if (literal? e)
                                (merge-with into var->joins {e-var [{:idx-fn #(idx/new-relation-virtual-index e-var [[e]] 1)}]})
                                var->joins)
                   var->joins (if (literal? v)
                                (merge-with into var->joins {v-var [{:idx-fn #(idx/new-relation-virtual-index v-var [[v]] 1)}]})
                                var->joins)]
               var->joins))
           var->joins))]))

(defn- validate-args [args]
  (let [ks (keys (first args))]
    (doseq [m args]
      (when-not (every? #(contains? m %) ks)
        (throw (IllegalArgumentException.
                (str "Argument maps need to contain the same keys as first map: " ks " " (keys m))))))))

(defn- arg-vars [args]
  (let [ks (keys (first args))]
    (set (for [k ks]
           (symbol (name k))))))

(defn- arg-joins [arg-vars e-vars var->joins]
  (if (seq arg-vars)
    (let [idx-id (gensym "args")
          join {:id idx-id
                :idx-fn #(idx/new-relation-virtual-index idx-id
                                                         []
                                                         (count arg-vars))}]
      [idx-id
       (->> arg-vars
            (reduce
             (fn [var->joins arg-var]
               (->> {arg-var
                     [(assoc join :name (symbol "crux.query.value" (name arg-var)))]}
                    (merge-with into var->joins)))
             var->joins))])
    [nil var->joins]))

(defn- pred-joins [pred-clauses v-var->range-constraints var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause+idx-ids var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [idx-id (gensym "pred-return")
                  join {:id idx-id
                        :idx-fn #(idx/new-relation-virtual-index idx-id
                                                                 []
                                                                 1
                                                                 [(get v-var->range-constraints return)])
                        :name (symbol "crux.query.value" (name return))}]
              [(conj pred-clause+idx-ids [pred-clause idx-id])
               (merge-with into var->joins {return [join]})])
            [(conj pred-clause+idx-ids [pred-clause])
             var->joins]))
        [[] var->joins])))

;; TODO: This is a naive, but not totally irrelevant measure. Aims to
;; bind variables as early and cheaply as possible.
(defn- clause-complexity [clause]
  (count (pr-str clause)))

(defn- single-e-var-triple? [vars where]
  (and (= 1 (count where))
       (let [[[type {:keys [e v]}]] where]
         (and (= :triple type)
              (contains? vars e)
              (logic-var? e)
              (literal? v)))))

(defn- or-joins [rules or-type or-clauses var->joins known-vars]
  (->> (sort-by clause-complexity or-clauses)
       (reduce
        (fn [[or-clause+idx-id+or-branches known-vars var->joins] clause]
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
                                        free-vars (set/difference or-vars known-vars)
                                        bound-vars (set/difference or-vars free-vars)]]
                              (do (when or-join?
                                    (doseq [var or-vars
                                            :when (not (contains? body-vars var))]
                                      (throw (IllegalArgumentException.
                                              (str "Or join variable never used: " var " " (pr-str clause))))))
                                  {:or-vars or-vars
                                   :free-vars free-vars
                                   :bound-vars bound-vars
                                   :where where
                                   :single-e-var-triple? (single-e-var-triple? bound-vars where)}))
                free-vars (:free-vars (first or-branches))
                idx-id (gensym "or-free-vars")
                join (when (seq free-vars)
                       {:id idx-id
                        :idx-fn #(idx/new-relation-virtual-index idx-id
                                                                 []
                                                                 (count free-vars))})]
            (when (not (apply = (map :or-vars or-branches)))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variables: " (pr-str clause)))))
            [(conj or-clause+idx-id+or-branches [clause idx-id or-branches])
             (into known-vars free-vars)
             (apply merge-with into var->joins (for [v free-vars]
                                                 {v [(assoc join :name (symbol "crux.query.value" (name v)))]}))]))
        [[] known-vars var->joins])))

(defrecord VarBinding [e-var var attr result-index join-depth result-name type value?])

;; NOTE: result-index is the index into join keys, it's the var's
;; position into vars-in-join-order. The join-depth is the depth at
;; which the var can first be accessed, that is, its e-var have had at
;; least one participating clause fully joined and is part of the
;; active result tuple. That is, its v-var, not necessarily this var,
;; has been joined as well. There are subtleties to this, so take this
;; explanation with a grain of salt.
(defn- build-var-bindings [var->attr v-var->e e->v-var var->values-result-index max-join-depth vars]
  (->> (for [var vars
             :let [e (get v-var->e var var)
                   join-depth (or (max (long (get var->values-result-index e -1))
                                       (long (get var->values-result-index (get e->v-var e) -1)))
                                  (dec (long max-join-depth)))
                   result-index (get var->values-result-index var)]]
         [var (map->VarBinding
               {:e-var e
                :var var
                :attr (get var->attr var)
                :result-index result-index
                :join-depth join-depth
                :result-name e
                :type :entity
                :value? false})])
       (into {})))

(defn- value-var-binding [var result-index type]
  (map->VarBinding
   {:var var
    :result-name (symbol "crux.query.value" (name var))
    :result-index result-index
    :join-depth result-index
    :type type
    :value? true}))

(defn- build-arg-var-bindings [var->values-result-index arg-vars]
  (->> (for [var arg-vars
             :let [result-index (get var->values-result-index var)]]
         [var (value-var-binding var result-index :arg)])
       (into {})))

(defn- build-pred-return-var-bindings [var->values-result-index pred-clauses]
  (->> (for [{:keys [return]} pred-clauses
             :when return
             :let [result-index (get var->values-result-index return)]]
         [return (value-var-binding return result-index :pred)])
       (into {})))

(defn- build-or-free-var-bindings [var->values-result-index or-clause+relation+or-branches]
  (->> (for [[_ _ or-branches] or-clause+relation+or-branches
             var (:free-vars (first or-branches))
             :let [result-index (get var->values-result-index var)]]
         [var (value-var-binding var result-index :or)])
       (into {})))

(defrecord BoundResult [var value doc])

(def ^:private ^ThreadLocal value-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn- bound-result-for-var ^crux.query.BoundResult [snapshot object-store var->bindings join-keys join-results var]
  (let [binding ^VarBinding (get var->bindings var)]
    (if (.value? binding)
      (BoundResult. var (get join-results (.result-name binding)) nil)
      (when-let [^EntityTx entity-tx (get join-results (.e-var binding))]
        (let [value-buffer (get join-keys (.result-index binding))
              content-hash (.content-hash entity-tx)
              doc (db/get-single-object object-store snapshot content-hash)
              values (idx/normalize-value (get doc (.attr binding)))
              value (if (or (nil? value-buffer)
                            (= (count values) 1))
                      (first values)
                      (loop [[x & xs] values]
                        (if (mem/buffers=? value-buffer (c/value->buffer x (.get value-buffer-tl)))
                          x
                          (when xs
                            (recur xs)))))]
          (BoundResult. var value doc))))))

(declare build-sub-query)

(defn- calculate-constraint-join-depth
  ([var->bindings vars]
   (calculate-constraint-join-depth var->bindings vars :join-depth))
  ([var->bindings vars var-k]
   (->> (for [var vars]
          (get-in var->bindings [var var-k] -1))
        (apply max -1)
        (long)
        (inc))))

(defn- validate-existing-vars [var->bindings clause vars]
  (doseq [var vars
          :when (not (contains? var->bindings var))]
    (throw (IllegalArgumentException.
            (str "Clause refers to unknown variable: "
                 var " " (pr-str clause))))))

(defn- build-pred-constraints [pred-clause+idx-ids var->bindings]
  (for [[{:keys [pred return] :as clause} idx-id] pred-clause+idx-ids
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)]]
    (do (validate-existing-vars var->bindings clause pred-vars)
        {:join-depth pred-join-depth
         :constraint-fn
         (fn pred-constraint [snapshot {:keys [object-store] :as db} idx-id->idx join-keys join-results]
           (let [[pred-fn & args] (for [arg (cons pred-fn args)]
                                    (if (logic-var? arg)
                                      (.value (bound-result-for-var snapshot object-store var->bindings join-keys join-results arg))
                                      arg))]
             (when-let [pred-result (apply pred-fn args)]
               (when return
                 (idx/update-relation-virtual-index! (get idx-id->idx idx-id) [[pred-result]]))
               join-results)))})))

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
(defn- or-single-e-var-triple-fast-path [snapshot {:keys [valid-time transact-time] :as db} {:keys [e a v] :as clause} args]
  (let [entity (get (first args) e)]
    (when (idx/or-known-triple-fast-path snapshot entity a v valid-time transact-time)
      [])))

(defn- build-branch-index->single-e-var-triple-fast-path-clause-with-buffers [or-branches]
  (->> (for [[branch-index {:keys [where
                                   single-e-var-triple?] :as or-branch}] (map-indexed vector or-branches)
             :when single-e-var-triple?
             :let [[[_ clause]] where]]
         [branch-index
          (-> clause
              (update :a c/->id-buffer)
              (update :v c/->value-buffer))])
       (into {})))

(def ^:private ^:dynamic *recursion-table* {})

;; TODO: This tabling mechanism attempts at avoiding infinite
;; recursion, but does not actually cache anything. Short-circuits
;; identical sub trees. Passes tests, unsure if this really works in
;; the general case. Depends on the eager expansion of rules for some
;; cases to pass. One alternative is maybe to try to cache the
;; sequence and reuse it, somehow detecting if it loops.
(defn- build-or-constraints
  [rule-name->rules or-clause+idx-id+or-branches
   var->bindings vars-in-join-order v-var->range-constraints stats]
  (for [[clause idx-id [{:keys [free-vars bound-vars]} :as or-branches]] or-clause+idx-id+or-branches
        :let [or-join-depth (calculate-constraint-join-depth var->bindings bound-vars)
              free-vars-in-join-order (filter (set free-vars) vars-in-join-order)
              has-free-vars? (boolean (seq free-vars))
              {:keys [rule-name]} (meta clause)
              branch-index->single-e-var-triple-fast-path-clause-with-buffers
              (build-branch-index->single-e-var-triple-fast-path-clause-with-buffers or-branches)]]
    (do (validate-existing-vars var->bindings clause bound-vars)
        {:join-depth or-join-depth
         :constraint-fn
         (fn or-constraint [snapshot {:keys [object-store] :as db} idx-id->idx join-keys join-results]
           (let [args (when (seq bound-vars)
                        [(->> (for [var bound-vars]
                                (.value (bound-result-for-var snapshot object-store var->bindings join-keys join-results var)))
                              (zipmap bound-vars))])
                 branch-results (for [[branch-index {:keys [where
                                                            single-e-var-triple?] :as or-branch}] (map-indexed vector or-branches)
                                      :let [cache-key (when rule-name
                                                        [rule-name branch-index (count free-vars) (set (mapv vals args))])
                                            cached-result (when cache-key
                                                            (get *recursion-table* cache-key))]]
                                  (with-open [snapshot (lru/new-cached-snapshot snapshot false)]
                                    (cond
                                      cached-result
                                      cached-result

                                      single-e-var-triple?
                                      (or-single-e-var-triple-fast-path
                                       snapshot
                                       db
                                       (get branch-index->single-e-var-triple-fast-path-clause-with-buffers branch-index)
                                       args)

                                      :else
                                      (binding [*recursion-table* (if cache-key
                                                                    (assoc *recursion-table* cache-key [])
                                                                    *recursion-table*)]
                                        (let [{:keys [n-ary-join
                                                      var->bindings]} (build-sub-query snapshot db where args rule-name->rules stats)]
                                          (when-let [idx-seq (seq (idx/layered-idx->seq n-ary-join))]
                                            (if has-free-vars?
                                              (vec (for [[join-keys join-results] idx-seq]
                                                     (vec (for [var free-vars-in-join-order]
                                                            (.value (bound-result-for-var snapshot object-store var->bindings join-keys join-results var))))))
                                              [])))))))]
             (when (seq (remove nil? branch-results))
               (when has-free-vars?
                 (let [free-results (->> branch-results
                                         (apply concat)
                                         (distinct)
                                         (vec))]
                   (idx/update-relation-virtual-index! (get idx-id->idx idx-id) free-results (map v-var->range-constraints free-vars-in-join-order))))
               join-results)))})))

;; TODO: Unification could be improved by using dynamic relations
;; propagating knowledge from the first var to the next. Currently
;; unification has to scan the values and check them as they get bound
;; and doesn't fully carry its weight compared to normal predicates.
(defn- build-unification-constraints [unify-clauses var->bindings]
  (for [{:keys [op args]
         :as clause} unify-clauses
        :let [unification-vars (filter logic-var? args)
              unification-join-depth (calculate-constraint-join-depth var->bindings unification-vars :result-index)
              args (vec (for [arg args]
                          (if (logic-var? arg)
                            arg
                            (->> (map c/->value-buffer (idx/normalize-value arg))
                                 (into (sorted-set-by mem/buffer-comparator))))))]]
    (do (validate-existing-vars var->bindings clause unification-vars)
        {:join-depth unification-join-depth
         :constraint-fn
         (fn unification-constraint [snapshot {:keys [object-store] :as db} idx-id->idx join-keys join-results]
           (let [values (for [arg args]
                          (if (logic-var? arg)
                            (let [{:keys [result-index]} (get var->bindings arg)]
                              (->> (get join-keys result-index)
                                   (sorted-set-by mem/buffer-comparator)))
                            arg))]
             (when (case op
                     == (boolean (not-empty (apply set/intersection values)))
                     != (empty? (apply set/intersection values)))
               join-results)))})))

(defn- build-not-constraints [rule-name->rules not-type not-clauses var->bindings stats]
  (for [not-clause not-clauses
        :let [[not-vars not-clause] (case not-type
                                      :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                            not-clause]
                                      :not-join [(:args not-clause)
                                                 (:body not-clause)])
              not-vars (remove blank-var? not-vars)
              not-join-depth (calculate-constraint-join-depth var->bindings not-vars)]]
    (do (validate-existing-vars var->bindings not-clause not-vars)
        {:join-depth not-join-depth
         :constraint-fn
         (fn not-constraint [snapshot {:keys [object-store] :as db} idx-id->idx join-keys join-results]
           (with-open [snapshot (lru/new-cached-snapshot snapshot false)]
             (let [args (when (seq not-vars)
                          [(->> (for [var not-vars]
                                  (.value (bound-result-for-var snapshot object-store var->bindings join-keys join-results var)))
                                (zipmap not-vars))])
                   {:keys [n-ary-join]} (build-sub-query snapshot db not-clause args rule-name->rules stats)]
               (when (empty? (idx/layered-idx->seq n-ary-join))
                 join-results))))})))

(defn- constrain-join-result-by-constraints [snapshot db idx-id->idx depth->constraints join-keys join-results]
  (reduce
   (fn [results constraint]
     (when results
       (constraint snapshot db idx-id->idx join-keys results)))
   join-results
   (get depth->constraints (count join-keys))))

(defn- potential-bpg-pair-vars [g vars]
  (for [var vars
        pair-var (dep/transitive-dependents g var)]
    pair-var))

(defn- calculate-join-order [pred-clauses or-clause+idx-id+or-branches var->joins arg-vars triple-join-deps]
  (let [g (->> (keys var->joins)
               (reduce
                (fn [g v]
                  (dep/depend g v ::root))
                triple-join-deps))
        g (reduce
           (fn [g {:keys [pred return] :as pred-clause}]
             (let [pred-vars (filter logic-var? (:args pred))
                   pred-vars (into pred-vars (potential-bpg-pair-vars triple-join-deps pred-vars))]
               (->> (for [pred-var pred-vars
                          :when return
                          return [return]]
                      [return pred-var])
                    (reduce
                     (fn [g [r a]]
                       (dep/depend g r a))
                     g))))
           g
           pred-clauses)
        g (reduce
           (fn [g [_ _ [{:keys [free-vars bound-vars]}]]]
             (let [bound-vars (into bound-vars (potential-bpg-pair-vars triple-join-deps bound-vars))]
               (->> (for [bound-var bound-vars
                          free-var free-vars]
                      [free-var bound-var])
                    (reduce
                     (fn [g [f b]]
                       (dep/depend g f b))
                     g))))
           g
           or-clause+idx-id+or-branches)
        join-order (dep/topo-sort g)]
    (vec (remove #{::root} join-order))))

(defn- rule-name->rules [rules]
  (group-by (comp :name :head) rules))

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

;; NOTE: this isn't exact, used to detect vars that can be bound
;; before an or sub query. Is there a better way to incrementally
;; build up the join order? Done until no new vars are found to catch
;; all vars, doesn't care about cyclic dependencies, these will be
;; caught by the real dependency check later.
(defn- add-pred-returns-bound-at-top-level [known-vars pred-clauses]
  (let [new-known-vars (->> pred-clauses
                            (reduce
                             (fn [acc {:keys [pred return]}]
                               (if (->> (cons (:pred-fn pred) (:args pred))
                                        (filter logic-var?)
                                        (set)
                                        (set/superset? acc))
                                 (conj acc return)
                                 acc))
                             known-vars))]
    (if (= new-known-vars known-vars)
      new-known-vars
      (recur new-known-vars pred-clauses))))

(defn- build-v-var->e [triple-clauses var->values-result-index]
  (->> (for [{:keys [e v] :as clause} triple-clauses
             :when (logic-var? v)]
         [v e])
       (sort-by (comp var->values-result-index second))
       (into {})))

(defn- compile-sub-query [where arg-vars rule-name->rules stats]
  (let [where (expand-rules where rule-name->rules {})
        {triple-clauses :triple
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
                unification-vars
                pred-vars
                pred-return-vars]} (collect-vars type->clauses)
        var->joins {}
        v-var->range-constraints (build-v-var-range-constraints e-vars range-clauses)
        v-range-vars (set (keys v-var->range-constraints))
        [triple-join-deps var->joins] (triple-joins triple-clauses
                                                    range-clauses
                                                    var->joins
                                                    arg-vars
                                                    stats)
        [args-idx-id var->joins] (arg-joins arg-vars
                                            e-vars
                                            var->joins)
        [pred-clause+idx-ids var->joins] (pred-joins pred-clauses v-var->range-constraints var->joins)
        known-vars (set/union e-vars v-vars arg-vars)
        known-vars (add-pred-returns-bound-at-top-level known-vars pred-clauses)
        [or-clause+idx-id+or-branches known-vars var->joins] (or-joins rule-name->rules
                                                                       :or
                                                                       or-clauses
                                                                       var->joins
                                                                       known-vars)
        [or-join-clause+idx-id+or-branches known-vars var->joins] (or-joins rule-name->rules
                                                                            :or-join
                                                                            or-join-clauses
                                                                            var->joins
                                                                            known-vars)
        or-clause+idx-id+or-branches (concat or-clause+idx-id+or-branches
                                             or-join-clause+idx-id+or-branches)
        join-depth (count var->joins)
        vars-in-join-order (calculate-join-order pred-clauses or-clause+idx-id+or-branches var->joins arg-vars triple-join-deps)
        arg-vars-in-join-order (filter (set arg-vars) vars-in-join-order)
        var->values-result-index (zipmap vars-in-join-order (range))
        v-var->e (build-v-var->e triple-clauses var->values-result-index)
        e->v-var (set/map-invert v-var->e)
        v-var->attr (->> (for [{:keys [e a v]} triple-clauses
                               :when (and (logic-var? v)
                                          (= e (get v-var->e v)))]
                           [v a])
                         (into {}))
        e-var->attr (zipmap e-vars (repeat :crux.db/id))
        var->attr (merge e-var->attr v-var->attr)
        var->bindings (merge (build-or-free-var-bindings var->values-result-index or-clause+idx-id+or-branches)
                             (build-pred-return-var-bindings var->values-result-index pred-clauses)
                             (build-arg-var-bindings var->values-result-index arg-vars)
                             (build-var-bindings var->attr
                                                 v-var->e
                                                 e->v-var
                                                 var->values-result-index
                                                 join-depth
                                                 (keys var->attr)))
        unification-constraints (build-unification-constraints unify-clauses var->bindings)
        not-constraints (build-not-constraints rule-name->rules :not not-clauses var->bindings stats)
        not-join-constraints (build-not-constraints rule-name->rules :not-join not-join-clauses var->bindings stats)
        pred-constraints (build-pred-constraints pred-clause+idx-ids var->bindings)
        or-constraints (build-or-constraints rule-name->rules or-clause+idx-id+or-branches
                                             var->bindings vars-in-join-order v-var->range-constraints stats)
        depth->constraints (->> (concat unification-constraints
                                        pred-constraints
                                        not-constraints
                                        not-join-constraints
                                        or-constraints)
                                (reduce
                                 (fn [acc {:keys [join-depth constraint-fn]}]
                                   (update acc join-depth (fnil conj []) constraint-fn))
                                 (vec (repeat join-depth nil))))]
    {:depth->constraints depth->constraints
     :v-var->range-constraints v-var->range-constraints
     :vars-in-join-order vars-in-join-order
     :var->joins var->joins
     :var->bindings var->bindings
     :arg-vars-in-join-order arg-vars-in-join-order
     :args-idx-id args-idx-id
     :attr-stats (select-keys stats (vals var->attr))}))

(defn- build-idx-id->idx [var->joins]
  (->> (for [[_ joins] var->joins
             {:keys [id idx-fn] :as join} joins
             :when id]
         [id idx-fn])
       (reduce
        (fn [acc [id idx-fn]]
          (if (contains? acc id)
            acc
            (assoc acc id (idx-fn))))
        {})))

(defn- build-sub-query [snapshot {:keys [kv query-cache object-store valid-time transact-time] :as db} where args rule-name->rules stats]
  ;; NOTE: this implies argument sets with different vars get compiled
  ;; differently.
  (let [arg-vars (arg-vars args)
        {:keys [depth->constraints
                vars-in-join-order
                v-var->range-constraints
                var->joins
                var->bindings
                arg-vars-in-join-order
                args-idx-id
                attr-stats]} (lru/compute-if-absent
                              query-cache
                              [where arg-vars rule-name->rules]
                              identity
                              (fn [_]
                                (compile-sub-query where arg-vars rule-name->rules stats)))
        idx-id->idx (build-idx-id->idx var->joins)
        constrain-result-fn (fn [join-keys join-results]
                              (constrain-join-result-by-constraints snapshot db idx-id->idx depth->constraints join-keys join-results))
        unary-join-index-groups (for [v vars-in-join-order]
                                  (for [{:keys [id idx-fn name] :as join} (get var->joins v)]
                                    (assoc (or (get idx-id->idx id) (idx-fn)) :name name)))]
    (doseq [[_ idx] idx-id->idx
            :when (instance? BinaryJoinLayeredVirtualIndex idx)]
      (update-binary-index! snapshot db idx vars-in-join-order v-var->range-constraints))
    (when (and (seq args) args-idx-id)
      (idx/update-relation-virtual-index!
       (get idx-id->idx args-idx-id)
       (vec (for [arg (distinct args)]
              (mapv #(arg-for-var arg %) arg-vars-in-join-order)))
       (mapv v-var->range-constraints arg-vars-in-join-order)))
    (log/debug :where (pr-str where))
    (log/debug :vars-in-join-order vars-in-join-order)
    (log/debug :attr-stats (pr-str attr-stats))
    (log/debug :var->bindings (pr-str var->bindings))
    (constrain-result-fn [] [])
    {:n-ary-join (-> (mapv idx/new-unary-join-virtual-index unary-join-index-groups)
                     (idx/new-n-ary-join-layered-virtual-index)
                     (idx/new-n-ary-constraining-layered-virtual-index constrain-result-fn))
     :var->bindings var->bindings}))

;; NOTE: For ascending sort, it might be possible to pick the right
;; join order so the resulting seq is already sorted, by ensuring the
;; first vars of the join order overlap with the ones in order
;; by. Depending on the query this might not be possible. For example,
;; when using or-join/rules the order from the sub queries cannot be
;; guaranteed. The order by vars must be in the set of bound vars for
;; all or statements in the query for this to work. This is somewhat
;; related to embedding or in the main query. Also, this sort is based
;; on the actual values, and not the byte arrays, which would give
;; different sort order for example for ids, where the hash used in
;; the indexes won't sort the same as the actual value. For this to
;; work well this would need to be revisited.
(defn- order-by-comparator [vars order-by]
  (let [var->index (zipmap vars (range))]
    (reify Comparator
      (compare [_ a b]
        (loop [diff 0
               [{:keys [var direction]} & order-by] order-by]
          (if (or (not (zero? diff))
                  (nil? var))
            diff
            (let [index (get var->index var)]
              (recur (long (cond-> (compare (get a index)
                                            (get b index))
                             (= :desc direction) -))
                     order-by))))))))

(defn normalize-query [q]
  (cond
    (vector? q) (into {} (for [[[k] v] (->> (partition-by keyword? q)
                                            (partition-all 2))]
                           [k (if (and (or (nat-int? (first v))
                                           (boolean? (first v)))
                                       (= 1 (count v)))
                                (first v)
                                (vec v))]))
    (string? q) (if-let [q (try
                             (c/read-edn-string-with-readers q)
                             (catch Exception e))]
                  (normalize-query q)
                  q)
    :else
    q))

(defn query-plan-for
  ([q]
   (query-plan-for q {}))
  ([q stats]
   (s/assert ::query q)
   (let [{:keys [where args rules]} (s/conform ::query q)]
     (compile-sub-query where (arg-vars args) (rule-name->rules rules) stats))))

(defn- build-full-results [{:keys [object-store entity-as-of-idx]} snapshot bound-result-tuple]
  (vec (for [^BoundResult bound-result bound-result-tuple
             :let [value (.value bound-result)]]
         (if-let [entity-tx (and (c/valid-id? value)
                                 (idx/entity-at entity-as-of-idx value))]
           (db/get-single-object object-store snapshot (.content-hash ^EntityTx entity-tx))
           value))))

(def default-query-timeout 30000)

(def default-entity-cache-size 10000)

(def ^:dynamic *with-entities-cache?* true)

(defrecord ConformedQuery [q-normalized q-conformed])

(defn- normalize-and-conform-query ^ConformedQuery [conform-cache q]
  (let [{:keys [args] :as q} (try
                               (normalize-query q)
                               (catch Exception e
                                 q))
        conformed-query (lru/compute-if-absent
                         conform-cache
                         (if (map? q)
                           (dissoc q :args)
                           q)
                         identity
                         (fn [q]
                           (let [q (normalize-query (s/assert ::query q))]
                             (->ConformedQuery q (s/conform ::query q)))))]
    (if args
      (do (s/assert ::args args)
          (-> conformed-query
              (assoc-in [:q-normalized :args] args)
              (assoc-in [:q-conformed :args] args)))
      conformed-query)))

;; TODO: Move future here to a bounded thread pool.
(defn q
  ([{:keys [kv conform-cache] :as db} q]
   (let [start-time (System/currentTimeMillis)
         q (.q-normalized (normalize-and-conform-query conform-cache q))
         query-future (future
                        (try
                          (with-open [snapshot (lru/new-cached-snapshot (kv/new-snapshot kv) true)]
                            (let [result-coll-fn (if (:order-by q)
                                                   (comp vec distinct)
                                                   set)
                                  result (result-coll-fn (crux.query/q db snapshot q))]
                              (log/debug :query-time-ms (- (System/currentTimeMillis) start-time))
                              (log/debug :query-result-size (count result))
                              result))
                          (catch ExceptionInfo e
                            e)
                          (catch IllegalArgumentException e
                            e)
                          (catch InterruptedException e
                            e)
                          (catch Throwable t
                            (log/error t "Exception caught while executing query.")
                            t)))
         result (or (try
                      (deref query-future (or (:timeout q) default-query-timeout) nil)
                      (catch InterruptedException e
                        (future-cancel query-future)
                        (throw e)))
                    (do (when-not (future-cancel query-future)
                          (throw (IllegalStateException. "Could not cancel query.")))
                        (throw (TimeoutException. "Query timed out."))))]
     (if (instance? Throwable result)
       (throw result)
       result)))
  ([{:keys [object-store conform-cache kv valid-time transact-time] :as db} snapshot q]
   (let [conformed-query (normalize-and-conform-query conform-cache q)
         q (.q-normalized conformed-query)
         q-conformed (.q-conformed conformed-query)
         {:keys [find where args rules offset limit order-by full-results?]} q-conformed
         stats (idx/read-meta kv :crux.kv/stats)]
     (log/debug :query (pr-str q))
     (validate-args args)
     (let [rule-name->rules (rule-name->rules rules)
           entity-as-of-idx (idx/new-entity-as-of-index snapshot valid-time transact-time)
           entity-as-of-idx (if *with-entities-cache?*
                              (lru/new-cached-index entity-as-of-idx default-entity-cache-size)
                              entity-as-of-idx)
           db (assoc db :entity-as-of-idx entity-as-of-idx)
           {:keys [n-ary-join
                   var->bindings]} (build-sub-query snapshot db where args rule-name->rules stats)]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Find refers to unknown variable: " var))))
       (doseq [{:keys [var]} order-by
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Order by refers to unknown variable: " var))))
       (cond->> (for [[join-keys join-results] (idx/layered-idx->seq n-ary-join)
                      :let [bound-result-tuple (for [var find]
                                                 (bound-result-for-var snapshot object-store var->bindings join-keys join-results var))]]
                  (if full-results?
                    (build-full-results db snapshot bound-result-tuple)
                    (mapv #(.value ^BoundResult %) bound-result-tuple)))
         order-by (cio/external-sort (order-by-comparator find order-by))
         (or offset limit) dedupe
         offset (drop offset)
         limit (take limit))))))

(defn entity-tx
  ([{:keys [kv] :as db} eid]
   (with-open [snapshot (kv/new-snapshot kv)]
     (entity-tx snapshot db eid)))
  ([snapshot {:keys [valid-time transact-time] :as db} eid]
   (c/entity-tx->edn (first (idx/entities-at snapshot [eid] valid-time transact-time)))))

(defn entity [{:keys [kv object-store] :as db} eid]
  (with-open [snapshot (kv/new-snapshot kv)]
    (let [entity-tx (entity-tx snapshot db eid)]
      (db/get-single-object object-store snapshot (:crux.db/content-hash entity-tx)))))

(defrecord QueryDatasource [kv query-cache conform-cache object-store valid-time transact-time entity-as-of-idx]
  ICruxDatasource
  (entity [this eid]
    (entity this eid))

  (entityTx [this eid]
    (entity-tx this eid))

  (newSnapshot [this]
    (lru/new-cached-snapshot (kv/new-snapshot (:kv this)) true))

  (q [this q]
    (crux.query/q this q))

  (q [this snapshot q]
    (crux.query/q this snapshot q))

  (historyAscending [this snapshot eid]
    (for [^EntityTx entity-tx (idx/entity-history-seq-ascending (kv/new-iterator snapshot) eid valid-time transact-time)]
      (assoc (c/entity-tx->edn entity-tx) :crux.db/doc (db/get-single-object object-store snapshot (.content-hash entity-tx)))))

  (historyDescending [this snapshot eid]
    (for [^EntityTx entity-tx (idx/entity-history-seq-descending (kv/new-iterator snapshot) eid valid-time transact-time)]
      (assoc (c/entity-tx->edn entity-tx) :crux.db/doc (db/get-single-object object-store snapshot (.content-hash entity-tx)))))

  (validTime [_]
    valid-time)

  (transactionTime [_]
    transact-time))

(defn db ^crux.api.ICruxDatasource [kv object-store valid-time transact-time]
  (->QueryDatasource kv
                     (lru/get-named-cache kv ::query-cache)
                     (lru/get-named-cache kv ::conform-cache)
                     object-store
                     valid-time
                     transact-time
                     nil))

(defn submitted-tx-updated-entity?
  ([kv object-store {:crux.tx/keys [tx-time] :as submitted-tx} eid]
   (submitted-tx-updated-entity? kv object-store submitted-tx tx-time eid))
  ([kv object-store {:crux.tx/keys [tx-id tx-time] :as submitted-tx} valid-time eid]
   (= tx-id (:crux.tx/tx-id (entity-tx (db kv object-store valid-time tx-time) eid)))))
