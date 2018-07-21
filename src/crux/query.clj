(ns crux.query
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.walk :as w]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db])
  (:import [java.util Date]))

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
                                      :x any?
                                      :y any?))))

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

(defn- cartesian-product [[x & xs]]
  (when (seq x)
    (for [a x
          bs (or (cartesian-product xs) [[]])]
      (cons a bs))))

(defn- blank-var? [v]
  (when (logic-var? v)
    (re-find #"^_\d*$" (name v))))

(defn- normalize-bgp-clause [{:keys [e v] :as clause}]
  (cond-> clause
    (or (blank-var? v)
        (nil? v))
    (assoc :v (gensym "_"))
    (blank-var? e)
    (assoc :e (gensym "_"))))

(def ^:private pred->built-in-range-pred {< (comp neg? compare)
                                          <= (comp not pos? compare)
                                          > (comp pos? compare)
                                          >= (comp not neg? compare)})

(def ^:private range->inverse-range '{< >=
                                      <= >
                                      > <=
                                      >= <})

(defn- rewrite-literal-e-literal-v-bgp-clause [{:keys [e v] :as bgp}]
  (let [v-var (gensym v)]
    {:bgp [(assoc bgp :v v-var)]
     :unify [{:op '== :x v-var :y (set (doc/normalize-value v))}]}))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         (if (= :bgp type)
           (let [{:keys [e v] :as bgp} (normalize-bgp-clause clause)]
             (if (and (literal? e) (literal? v))
               (rewrite-literal-e-literal-v-bgp-clause bgp)
               {type [bgp]}))
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
     :unification-vars (set (for [{:keys [x y]} unify-clauses
                                  arg [x y]
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

(defn- all-args-for-var [args var]
  (for [arg args]
    (or (get arg (symbol (name var)))
        (get arg (keyword (name var))))))

(defn- e-var-literal-v-joins [snapshot object-store e-var->literal-v-clauses var->joins arg-vars args business-time transact-time]
  (->> e-var->literal-v-clauses
       (reduce
        (fn [var->joins [e-var clauses]]
          (if (contains? arg-vars e-var)
            (let [entities (all-args-for-var args e-var)
                  idx (doc/new-shared-literal-attribute-for-known-entities-virtual-index
                       object-store
                       snapshot
                       entities
                       (vec (for [{:keys [a v]} clauses]
                              [a v]))
                       business-time
                       transact-time)]
              (merge-with into var->joins {e-var [(assoc idx :name e-var)]}))
            (let [idx (doc/new-shared-literal-attribute-entities-virtual-index
                       snapshot
                       (vec (for [{:keys [a v]} clauses]
                              [a v]))
                       business-time
                       transact-time)]
              (merge-with into var->joins {e-var [(assoc idx :name e-var)]}))))
        var->joins)))

(defn- e-var-v-var-joins [snapshot object-store e-var+v-var->join-clauses v-var->range-constriants var->joins arg-vars args business-time transact-time]
  (->> e-var+v-var->join-clauses
       (reduce
        (fn [var->joins [[e-var v-var] clauses]]
          (let [indexes (for [{:keys [a]} clauses]
                          (if (contains? arg-vars e-var)
                            (let [entities (all-args-for-var args e-var)]
                              (assoc (doc/new-entity-attribute-value-known-entities-virtual-index
                                      object-store
                                      snapshot
                                      entities
                                      a
                                      (get v-var->range-constriants v-var)
                                      business-time
                                      transact-time)
                                     :name e-var))
                            (assoc (doc/new-entity-attribute-value-virtual-index
                                    snapshot
                                    a
                                    (get v-var->range-constriants v-var)
                                    business-time
                                    transact-time)
                                   :name e-var)))]
            (merge-with into var->joins {v-var (vec indexes)})))
        var->joins)))

(defn- v-var-literal-e-joins [snapshot object-store v-var->literal-e-clauses v-var->range-constriants var->joins business-time transact-time]
  (->> v-var->literal-e-clauses
       (reduce
        (fn [var->joins [v-var clauses]]
          (let [indexes (for [{:keys [e a]} clauses]
                          (assoc (doc/new-literal-entity-attribute-values-virtual-index
                                  object-store
                                  snapshot
                                  e
                                  a
                                  (get v-var->range-constriants v-var)
                                  business-time
                                  transact-time)
                                 :name e))]
            (merge-with into var->joins {v-var (vec indexes)})))
        var->joins)))

(defn- arg-vars [args]
  (let [ks (keys (first args))]
    (doseq [m args]
      (when-not (every? #(contains? m %) ks)
        (throw (IllegalArgumentException.
                (str "Argument maps need to contain the same keys as first map: " ks " " (keys m))))))
    (set (for [k ks]
           (symbol (name k))))))

(defn- arg-joins [snapshot args e-vars v-var->range-constriants var->joins business-time transact-time]
  (let [arg-keys-in-join-order (sort (keys (first args)))
        arg-vars (arg-vars args)
        relation (doc/new-relation-virtual-index (gensym "args")
                                                 (for [arg args]
                                                   (mapv arg arg-keys-in-join-order))
                                                 (count arg-keys-in-join-order)
                                                 (mapv v-var->range-constriants arg-vars))]
    (->> arg-vars
         (reduce
          (fn [var->joins arg-var]
            (->> {arg-var
                  (cond-> [(assoc relation :name (symbol "crux.query.arg" (name arg-var)))]
                    (and (not (contains? var->joins arg-var))
                         (contains? e-vars arg-var))
                    (conj (assoc (doc/new-entity-attribute-value-virtual-index
                                  snapshot
                                  :crux.db/id
                                  nil
                                  business-time
                                  transact-time)
                                 :name arg-var)))}
                 (merge-with into var->joins)))
          var->joins))))

(defn- pred-joins [pred-clauses v-var->range-constriants var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause->relation var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [relation (doc/new-relation-virtual-index (gensym "preds")
                                                           []
                                                           1
                                                           [(get v-var->range-constriants return)])]
              [(assoc pred-clause->relation pred-clause relation)
               (->> {return
                     [(assoc relation :name (symbol "crux.query.pred" (name return)))]}
                    (merge-with into var->joins))])
            [pred-clause->relation var->joins]))
        [{} var->joins])))

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
                relation (doc/new-relation-virtual-index (gensym "or-vars")
                                                         []
                                                         (count free-vars))]
            (when (not (apply = (map :or-vars or-branches)))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variables: " (pr-str clause)))))
            [(conj or-clause+relation+or-branches [clause relation or-branches])
             (into known-vars free-vars)
             (apply merge-with into var->joins (for [v free-vars]
                                                 {v [(assoc relation :name (symbol "crux.query.or" (name v)))]}))]))
        [[] known-vars var->joins])))

(defn- build-var-bindings [var->attr v-var->e e->v-var var->values-result-index e-var->leaf-v-var-clauses join-depth vars]
  (->> (for [var vars
             :let [e (get v-var->e var var)
                   leaf-var? (contains? e-var->leaf-v-var-clauses e)
                   result-index (get var->values-result-index var)]]
         [var {:e-var e
               :var var
               :attr (get var->attr var)
               :result-index result-index
               :join-depth (or result-index
                               (get var->values-result-index (get e->v-var e))
                               (dec join-depth))
               :result-name (if leaf-var?
                              var
                              e)
               :type (if leaf-var?
                       :entity-leaf
                       :entity)
               :required-attrs (some->> (get e-var->leaf-v-var-clauses e)
                                        (not-empty)
                                        (map :a)
                                        (set))}])
       (into {})))

(defn- build-arg-var-bindings [var->values-result-index arg-vars]
  (->> (for [var arg-vars
             :let [result-index (get var->values-result-index var)]]
         [var {:var var
               :result-name (symbol "crux.query.arg" (name var))
               :result-index result-index
               :join-depth result-index
               :type :arg}])
       (into {})))

(defn- build-pred-return-var-bindings [var->values-result-index pred-clauses]
  (->> (for [{:keys [return]} pred-clauses
             :when return
             :let [result-index (get var->values-result-index return)]]
         [return {:var return
                  :result-name (symbol "crux.query.pred" (name return))
                  :result-index result-index
                  :join-depth result-index
                  :type :pred}])
       (into {})))

(defn- build-or-var-bindings [var->values-result-index or-clause+relation+or-branches]
  (->> (for [[_ _ or-branches] or-clause+relation+or-branches
             var (:free-vars (first or-branches))
             :let [result-index (get var->values-result-index var)]]
         [var {:var var
               :result-name (symbol "crux.query.or" (name var))
               :result-index result-index
               :join-depth result-index
               :type :or}])
       (into {})))

(defn- consistent-tuple? [tuple]
  (->> (for [[_ vs] (group-by :result-name tuple)
             :when (every? #{:entity :entity-leaf} (map :type vs))]
         (apply = (map :entity vs)))
       (every? true?)))

;; TODO: This should in theory not be necessary, it's an artefact of
;; how there can be several entities with the same value returned, but
;; only some of them might match a predicate. As the results currently
;; is a map of variables to sets of values, there's no way to
;; constrain across two variables. This used to work when there were
;; leaf predicates, which did fire on each actual row at the end.
(defn- valid-sub-tuple? [join-results tuple]
  (let [{:keys [valid-sub-value-groups]} (meta join-results)]
    (->> (for [valid-sub-value-group valid-sub-value-groups]
           (some #(let [ks (set/intersection (set (keys %))
                                             (set (keys tuple)))]
                    (= (select-keys % ks) (select-keys tuple ks)))
                 valid-sub-value-group))
         (every? true?))))

(defn- bound-result->join-result [{:keys [result-name value? type entity value] :as result}]
  {result-name
   #{(if (and value? (not= :entity-leaf type))
       value
       entity)}})

(defn- bound-results-for-var [object-store var->bindings join-keys join-results var]
  (let [{:keys [e-var var attr result-index result-name required-attrs type]} (get var->bindings var)
        bound-var? (and (= :entity-leaf type)
                        (not= e-var result-name)
                        (contains? join-results result-name))]
    (cond
      (contains? #{:arg :pred :or} type)
      (let [results (get join-results result-name)]
        (for [value results]
          {:value value
           :result-name result-name
           :type type
           :value? true}))

      bound-var?
      (let [results (get join-results result-name)]
        (for [value results]
          {:value value
           :result-name var
           :type :bound
           :value? true}))

      :else
      (let [entities (get join-results e-var)
            content-hashes (map :content-hash entities)
            content-hash->doc (db/get-objects object-store content-hashes)
            value-bytes (get join-keys result-index)]
        (for [[entity doc] (map vector entities (map content-hash->doc content-hashes))
              :when (or (empty? required-attrs)
                        (set/subset? required-attrs (set (keys doc))))
              value (doc/normalize-value (get doc attr))
              :when (or (nil? value-bytes)
                        (bu/bytes=? value-bytes (idx/value->bytes value)))]
          {:value value
           :e-var e-var
           :v-var var
           :attr attr
           :doc doc
           :entity entity
           :result-name result-name
           :type type
           :value? (= :entity-leaf type)})))))

(defn- calculate-constraint-join-depth [var->bindings vars]
  (->> (for [var vars]
         (get-in var->bindings [var :join-depth] -1))
       (apply max -1)
       (inc)))

(defn- build-pred-constraints [object-store pred-clauses var->bindings pred-clause->relation]
  (for [{:keys [pred return] :as clause} pred-clauses
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              needs-valid-sub-value-group? (> (count (distinct (filter logic-var? args))) 1)
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)]]
    (do (doseq [var pred-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Predicate refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn [join-keys join-results]
          (if (= (count join-keys) pred-join-depth)
            (let [arg-tuples (if (empty? args)
                               [{}]
                               (cartesian-product
                                (for [arg args]
                                  (if (logic-var? arg)
                                    (bound-results-for-var object-store var->bindings join-keys join-results arg)
                                    [{:value arg
                                      :literal-arg? true}]))))
                  pred-result+result-maps+args-tuple (for [args-tuple arg-tuples
                                                           :when (or (empty? args-tuple)
                                                                     (consistent-tuple? args-tuple)
                                                                     (valid-sub-tuple? join-results args-tuple))
                                                           pred-fn (if (logic-var? pred-fn)
                                                                     (map :value (bound-results-for-var object-store var->bindings join-keys join-results pred-fn))
                                                                     [pred-fn])
                                                           :let [pred-result (apply pred-fn (map :value args-tuple))]
                                                           :when pred-result]
                                                       [pred-result
                                                        (->> (for [{:keys [result-name value type literal-arg?] :as result} args-tuple
                                                                   :when (not literal-arg?)]
                                                               (if (= :entity-leaf type)
                                                                 {result-name #{value}}
                                                                 (bound-result->join-result result)))
                                                             (apply merge-with into))
                                                        (when needs-valid-sub-value-group?
                                                          (->> (for [[arg {:keys [value literal-arg?]}] (map vector args args-tuple)
                                                                     :when (not literal-arg?)]
                                                                 [arg value])
                                                               (into {})))])]
              (when return
                (doc/update-relation-virtual-index! (get pred-clause->relation clause)
                                                    (->> (for [[pred-result] pred-result+result-maps+args-tuple]
                                                           [pred-result])
                                                         (distinct)
                                                         (vec))))
              (when-let [join-results (some->> (mapv second pred-result+result-maps+args-tuple)
                                               (apply merge-with into)
                                               (merge join-results))]
                (if needs-valid-sub-value-group?
                  (->> (for [[_ _ args-tuple] pred-result+result-maps+args-tuple]
                         args-tuple)
                       (set)
                       (vary-meta join-results update :valid-sub-value-groups conj))
                  join-results)))
            join-results)))))

(defn- single-e-var-bgp? [vars where]
  (and (= 1 (count where))
       (let [[[type {:keys [e v]}]] where]
         (and (= :bgp type)
              (contains? vars e)
              (logic-var? e)
              (literal? v)))))

;; TODO: potentially the way to do this is to pass join results down
;; (either via a variable, an atom or a binding), and if a sub query
;; doesn't change it, terminate that branch.
(defn- build-or-constraints [{:keys [business-time transact-time] :as db} snapshot object-store rules or-clause+relation+or-branches var->bindings
                             vars-in-join-order v-var->range-constriants sub-query-result-cache]
  (for [[clause relation [{:keys [free-vars bound-vars]} :as or-branches]] or-clause+relation+or-branches
        :let [or-join-depth (calculate-constraint-join-depth var->bindings bound-vars)
              free-vars-in-join-order (filter (set free-vars) vars-in-join-order)
              rule-name (:rule-name (meta clause))]]
    (do (doseq [var bound-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Or refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn [join-keys join-results]
          (if (= (count join-keys) or-join-depth)
            (let [tuples (for [tuple (cartesian-product
                                      (for [var bound-vars]
                                        (bound-results-for-var object-store var->bindings join-keys join-results var)))
                               :when (and (consistent-tuple? tuple)
                                          (valid-sub-tuple? join-results tuple))]
                           tuple)
                  free-results+bound-results
                  (let [free-results+bound-results
                        (->> (for [[branch-index {:keys [where] :as or-branch}] (map-indexed vector or-branches)
                                   tuple (or (seq tuples)
                                             (when (seq free-vars)
                                               [{}]))
                                   :let [args (if (seq bound-vars)
                                                [(zipmap bound-vars (map :value tuple))]
                                                [])
                                         bound-results (->> (for [result tuple]
                                                              (bound-result->join-result result))
                                                            (apply merge-with into))
                                         cache-key (when rule-name
                                                     [:rule-results rule-name branch-index (count free-vars) (vec (for [tuple tuples]
                                                                                                                    (mapv :value tuple)))])]]
                               (if (single-e-var-bgp? bound-vars where)
                                 (let [[[_ {:keys [e a v] :as clause}]] where
                                       entities (mapv e args)
                                       idx (doc/new-shared-literal-attribute-for-known-entities-virtual-index
                                            object-store snapshot entities [[a v]] business-time transact-time)]
                                   (when (seq (doc/idx->seq idx))
                                     [(with-meta
                                        [[]
                                         bound-results]
                                        {:free-vars-in-join-order free-vars-in-join-order
                                         :bound-vars bound-vars})]))
                                 (or (get @sub-query-result-cache cache-key)
                                     (let [_ (when cache-key
                                               (swap! sub-query-result-cache assoc cache-key []))
                                           {:keys [n-ary-join
                                                   var->bindings
                                                   var->joins]} (build-sub-query snapshot db [] where args rules sub-query-result-cache)
                                           result (vec (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join (count var->joins))]
                                                         (with-meta
                                                           [(vec (for [tuple (cartesian-product
                                                                              (for [var free-vars-in-join-order]
                                                                                (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                                                       :when (and (consistent-tuple? tuple)
                                                                                  (valid-sub-tuple? join-results tuple))]
                                                                   (mapv :value tuple)))
                                                            bound-results]
                                                           {:free-vars-in-join-order free-vars-in-join-order
                                                            :bound-vars bound-vars})))]
                                       (when cache-key
                                         (swap! sub-query-result-cache assoc cache-key result))
                                       result))))
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
                (doc/update-relation-virtual-index! relation free-results (map v-var->range-constriants free-vars)))

              (if (empty? bound-vars)
                (when (seq free-results)
                  join-results)
                (when (seq bound-results)
                  (merge join-results bound-results))))
            join-results)))))

(defn- build-unification-preds [unify-clauses var->bindings]
  (for [{:keys [op x y]
         :as clause} unify-clauses]
    (do (doseq [arg [x y]
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Unification refers to unknown variable: "
                       arg " " (pr-str clause)))))
        (fn [join-keys join-results]
          (let [[x y] (for [arg [x y]]
                        (if (logic-var? arg)
                          (let [{:keys [e-var result-index]} (get var->bindings arg)]
                            (or (some->> (get join-keys result-index)
                                         (sorted-set-by bu/bytes-comparator))
                                (some->> (get join-results e-var)
                                         (map (comp idx/id->bytes :eid))
                                         (into (sorted-set-by bu/bytes-comparator)))))
                          (->> (map idx/value->bytes (doc/normalize-value arg))
                               (into (sorted-set-by bu/bytes-comparator)))))]
            (if (and x y)
              (case op
                == (boolean (not-empty (set/intersection x y)))
                != (empty? (set/intersection x y)))
              true))))))

(defn- build-not-constraints [db snapshot object-store rules not-type not-clauses var->bindings sub-query-result-cache]
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
        (fn [join-keys join-results]
          (if (= (count join-keys) not-join-depth)
            (let [args (vec (for [tuple (cartesian-product
                                         (for [var not-vars]
                                           (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                  :when (and (consistent-tuple? tuple)
                                             (valid-sub-tuple? join-results tuple))]
                              (zipmap not-vars (map :value tuple))))
                  parent-join-keys join-keys
                  parent-var->bindings var->bindings
                  {:keys [n-ary-join
                          var->bindings
                          var->joins]} (build-sub-query snapshot db [] not-clause args rules sub-query-result-cache)]
              (->> (doc/layered-idx->seq n-ary-join (count var->joins))
                   (reduce
                    (fn [parent-join-results [join-keys join-results]]
                      (let [results (for [var not-vars]
                                      (bound-results-for-var object-store var->bindings join-keys join-results var))
                            not-var->values (zipmap not-vars
                                                    (for [result results]
                                                      (->> result
                                                           (map :value)
                                                           (set))))
                            results-to-remove (->> (for [[var not-vs] not-var->values
                                                         :let [parent-results (bound-results-for-var object-store parent-var->bindings
                                                                                                     parent-join-keys parent-join-results var)]
                                                         {:keys [value] :as result} parent-results
                                                         :when (contains? not-vs value)]
                                                     (bound-result->join-result result))
                                                   (apply merge-with into))]
                        (some->> (merge-with set/difference parent-join-results results-to-remove)
                                 (filter (comp seq val))
                                 (not-empty)
                                 (into {}))))
                    join-results)))
            join-results)))))

(defn- constrain-join-result-by-unification [unification-preds join-keys join-results]
  (when (->> (for [pred unification-preds]
               (pred join-keys join-results))
             (every? true?))
    join-results))

(defn- constrain-join-result-by-constraints [constraints join-keys join-results]
  (reduce
   (fn [results constraint]
     (when results
       (constraint join-keys results)))
   join-results
   constraints))

(defn- constrain-join-result-by-join-keys [var->bindings shared-e-v-vars join-keys join-results]
  (->> (for [e-var shared-e-v-vars
             :let [eid-bytes (get join-keys (get-in var->bindings [e-var :result-index]))]
             :when eid-bytes
             entity (get join-results e-var)
             :when (not (bu/bytes=? eid-bytes (idx/id->bytes entity)))]
         {e-var #{entity}})
       (apply merge-with set/difference join-results)))

(defn- calculate-join-order [pred-clauses or-clause+relation+or-branches var->joins e->v-var]
  (let [vars-in-join-order (vec (keys var->joins))
        var->index (zipmap vars-in-join-order (range))
        ;; TODO: This is simplistic, really has to calculate
        ;; dependency graph between args and returns, also for the
        ;; ors.
        preds (for [{:keys [pred return] :as pred-clause} (reverse (sort-by :return pred-clauses))
                    :when return]
                ["Predicate" pred-clause (filter logic-var? (:args pred)) [return]])
        ors (for [[or-clause _ [{:keys [free-vars bound-vars]}]] or-clause+relation+or-branches
                  :when (not-empty free-vars)]
              ["Or" or-clause bound-vars free-vars])]
    (->> (concat preds ors)
         (reduce
          (fn [[vars-in-join-order seen-returns var->index]
               [msg clause args returns]]
            (->> returns
                 (reduce
                  (fn [[vars-in-join-order seen-returns var->index] return]
                    ;; TODO: What is correct here if the argument
                    ;; isn't in the join? We attempt to fallback to
                    ;; the v join of a an e, but unsure if this is
                    ;; correct. Does not work to set result-index to
                    ;; this, as this is used as an index into
                    ;; join-keys which contains the real value.
                    (let [max-dependent-var-index (->> (map #(or (get var->index %)
                                                                 (get var->index (get e->v-var %))) args)
                                                       (remove nil?)
                                                       (reduce max -1))
                          return-index (get var->index return)
                          seen-returns (conj seen-returns return)]
                      (when (some seen-returns args)
                        (throw (IllegalArgumentException.
                                (str msg " has circular dependency: " (pr-str clause)))))
                      (if (> max-dependent-var-index return-index)
                        (let [dependent-var (get vars-in-join-order max-dependent-var-index)
                              vars-in-join-order (-> vars-in-join-order
                                                     (assoc return-index dependent-var)
                                                     (assoc max-dependent-var-index return))]
                          [vars-in-join-order
                           seen-returns
                           (zipmap vars-in-join-order (range))])
                        [vars-in-join-order seen-returns var->index])))
                  [vars-in-join-order seen-returns var->index])))
          [vars-in-join-order #{} var->index])
         (first))))

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
               (let [seen-rule-branches+expanded-rules (for [[branch-index [rule-args body]] (map-indexed vector rule-args+body)
                                                             :let [cache-key [:seen-rule-branches rule-name branch-index (:args clause)]]
                                                             :when (zero? (get-in recursion-cache cache-key 0))
                                                             :let [rule-arg->query-arg (zipmap rule-args (:args clause))
                                                                   body-vars (->> (collect-vars (normalize-clauses body))
                                                                                  (vals)
                                                                                  (reduce into #{}))
                                                                   body-var->hidden-var (zipmap body-vars
                                                                                                (map gensym body-vars))]]
                                                         [cache-key
                                                          (w/postwalk-replace (merge body-var->hidden-var rule-arg->query-arg) body)])
                     seen-rule-branches (map first seen-rule-branches+expanded-rules)
                     expanded-rules (map second seen-rule-branches+expanded-rules)
                     recursion-cache (reduce (fn [cache cache-key]
                                               (update-in cache cache-key (fnil inc 0)))
                                             recursion-cache
                                             seen-rule-branches)
                     cache-key [:seen-rules rule-name]
                     expanded-rules (if (< (get-in recursion-cache cache-key 0) 2)
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

(defn- build-sub-query [snapshot {:keys [kv object-store business-time transact-time] :as db} find where args rules sub-query-result-cache]
  (let [rule-name->rules (group-by (comp :name :head) rules)
        where (expand-rules where rule-name->rules {})
        {bgp-clauses :bgp
         range-clauses :range
         pred-clauses :pred
         unify-clauses :unify
         not-clauses :not
         not-join-clauses :not-join
         or-clauses :or
         or-join-clauses :or-join
         rule-clauses :rule
         :as type->clauses} (normalize-clauses where)
        {:keys [e-vars
                v-vars
                unification-vars
                not-vars
                pred-vars
                pred-return-vars
                rule-vars]} (collect-vars type->clauses)
        e->v-var-clauses (->> (for [{:keys [v] :as clause} bgp-clauses
                                    :when (logic-var? v)]
                                clause)
                              (group-by :e))
        v->v-var-clauses (->> (for [{:keys [v] :as clause} bgp-clauses
                                    :when (logic-var? v)]
                                clause)
                              (group-by :v))
        v-var->e (->> (for [[e clauses] e->v-var-clauses
                            {:keys [e v]} clauses]
                        [v e])
                      (into {}))
        e-var->literal-v-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                            :when (and (logic-var? e)
                                                       (literal? v))]
                                        clause)
                                      (group-by :e))
        v-var->literal-e-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                            :when (and (db-ident? e)
                                                       (logic-var? v))]
                                        clause)
                                      (group-by :v))
        arg-vars (arg-vars args)
        var->joins (sorted-map)
        var->joins (e-var-literal-v-joins snapshot
                                          object-store
                                          e-var->literal-v-clauses
                                          var->joins
                                          arg-vars
                                          args
                                          business-time
                                          transact-time)
        non-leaf-v-vars (set/union unification-vars not-vars e-vars arg-vars rule-vars)
        leaf-v-var? (fn [e v]
                      (and (= 1 (count (get v->v-var-clauses v)))
                           (or (contains? e-var->literal-v-clauses e)
                               (contains? v-var->literal-e-clauses v))
                           (not (contains? non-leaf-v-vars v))))
        e-var+v-var->join-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                             :when (and (logic-var? e)
                                                        (logic-var? v)
                                                        (not (leaf-v-var? e v)))]
                                         clause)
                                       (group-by (juxt :e :v)))
        e-var->leaf-v-var-clauses (->> (for [{:keys [e a v] :as clause} bgp-clauses
                                             :when (and (logic-var? e)
                                                        (logic-var? v)
                                                        (leaf-v-var? e v))]
                                         clause)
                                       (group-by :e))
        v-var->range-constriants (build-v-var-range-constraints e-vars range-clauses)
        var->joins (e-var-v-var-joins snapshot
                                      object-store
                                      e-var+v-var->join-clauses
                                      v-var->range-constriants
                                      var->joins
                                      arg-vars
                                      args
                                      business-time
                                      transact-time)
        var->joins (v-var-literal-e-joins snapshot
                                          object-store
                                          v-var->literal-e-clauses
                                          v-var->range-constriants
                                          var->joins
                                          business-time
                                          transact-time)
        var->joins (arg-joins snapshot
                              args
                              e-vars
                              v-var->range-constriants
                              var->joins
                              business-time
                              transact-time)
        [pred-clause->relation var->joins] (pred-joins pred-clauses v-var->range-constriants var->joins)
        known-vars (set/union e-vars v-vars pred-return-vars arg-vars)
        [or-clause+relation+or-branches known-vars var->joins] (or-joins snapshot
                                                                         db
                                                                         rules
                                                                         :or
                                                                         or-clauses
                                                                         var->joins
                                                                         known-vars)
        [or-join-clause+relation+or-branches known-vars var->joins] (or-joins snapshot
                                                                              db
                                                                              rules
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
        e->v-var (set/map-invert v-var->e)
        vars-in-join-order (calculate-join-order pred-clauses or-clause+relation+or-branches var->joins e->v-var)
        var->values-result-index (zipmap vars-in-join-order (range))
        var->bindings (build-var-bindings var->attr
                                          v-var->e
                                          e->v-var
                                          var->values-result-index
                                          e-var->leaf-v-var-clauses
                                          join-depth
                                          (keys var->attr))
        var->bindings (merge (build-pred-return-var-bindings var->values-result-index pred-clauses)
                             (build-arg-var-bindings var->values-result-index arg-vars)
                             (merge (build-or-var-bindings var->values-result-index or-clause+relation+or-branches)
                                    var->bindings))
        unification-preds (vec (build-unification-preds unify-clauses var->bindings))
        not-constraints (vec (concat (build-not-constraints db snapshot object-store rules :not not-clauses var->bindings sub-query-result-cache)
                                     (build-not-constraints db snapshot object-store rules :not-join not-join-clauses var->bindings sub-query-result-cache)))
        pred-constraints (vec (build-pred-constraints object-store pred-clauses var->bindings pred-clause->relation))
        or-constraints (vec (build-or-constraints db snapshot object-store rules or-clause+relation+or-branches
                                                  var->bindings vars-in-join-order v-var->range-constriants sub-query-result-cache))
        shared-e-v-vars (set/intersection e-vars v-vars)
        constrain-result-fn (fn [max-ks result]
                              (some->> (doc/constrain-join-result-by-empty-names max-ks result)
                                       (constrain-join-result-by-join-keys var->bindings shared-e-v-vars max-ks)
                                       (constrain-join-result-by-unification unification-preds max-ks)
                                       (constrain-join-result-by-constraints not-constraints max-ks)
                                       (constrain-join-result-by-constraints pred-constraints max-ks)
                                       (constrain-join-result-by-constraints or-constraints max-ks)))
        joins (map var->joins vars-in-join-order)]
    (constrain-result-fn [] [])
    {:n-ary-join (-> (mapv doc/new-unary-join-virtual-index joins)
                     (doc/new-n-ary-join-layered-virtual-index)
                     (doc/new-n-ary-constraining-layered-virtual-index constrain-result-fn))
     :var->bindings var->bindings
     :var->joins var->joins
     :vars-in-join-order vars-in-join-order}))

(defn q
  ([{:keys [kv] :as db} q]
   (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot kv) true)]
     (set (crux.query/q snapshot db q))))
  ([snapshot {:keys [kv object-store business-time transact-time] :as db} q]
   (let [{:keys [find where args rules] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (IllegalArgumentException.
               (str "Invalid input: " (s/explain-str :crux.query/query q)))))
     (let [sub-query-result-cache (atom {})
           {:keys [n-ary-join
                   var->bindings
                   var->joins]} (build-sub-query snapshot db find where args rules sub-query-result-cache)]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Find refers to unknown variable: " var))))
       (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join (count var->joins))
             tuple (cartesian-product
                    (for [var find]
                      (bound-results-for-var object-store var->bindings join-keys join-results var)))
             :let [values (mapv :value tuple)]
             :when (and (consistent-tuple? tuple)
                        (valid-sub-tuple? join-results (zipmap find values)))]
         (with-meta
           values
           (zipmap (map :e-var tuple) tuple)))))))

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
   (db kv (Date.)))
  ([kv business-time]
   (->QueryDatasource kv
                      (doc/new-cached-object-store kv)
                      business-time
                      (Date.)))
  ([kv business-time transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout)
   (->QueryDatasource kv
                      (doc/new-cached-object-store kv)
                      business-time
                      transact-time)))
