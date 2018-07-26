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

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         {type [(case type
                  :bgp (normalize-bgp-clause clause)
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
                  clause)]})
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

(defn- arg-for-var [arg var]
  (or (get arg (symbol (name var)))
      (get arg (keyword (name var)))))

(defn- all-args-for-var [args var]
  (map #(arg-for-var % var) args))

(defn- update-binary-index [snapshot {:keys [business-time transact-time]} binary-idx vars-in-join-order v-var->range-constriants]
  (let [{:keys [clause names]} (meta binary-idx)
        {:keys [e a v]} clause
        order (filter (set (vals names)) vars-in-join-order)]
    (if (= (:v names) (first order))
      (let [v-doc-idx (-> (doc/new-doc-attribute-value-entity-value-index (ks/new-iterator snapshot) a)
                          (doc/wrap-with-range-constraints (get v-var->range-constriants v)))
            e-idx (doc/new-entity-attribute-value-virtual-index
                   snapshot
                   (doc/new-doc-attribute-value-entity-entity-index (ks/new-iterator snapshot) v-doc-idx)
                   nil
                   business-time
                   transact-time)]
        ;; (prn :ave v e)
        (doc/update-binary-join-order! binary-idx v-doc-idx e-idx))
      (let [e-doc-idx (doc/new-doc-attribute-entity-value-entity-index (ks/new-iterator snapshot) a)
            v-idx (doc/new-entity-attribute-value-virtual-index
                   snapshot
                   (-> (doc/new-doc-attribute-entity-value-value-index (ks/new-iterator snapshot) e-doc-idx)
                       (doc/wrap-with-range-constraints (get v-var->range-constriants v)))
                   nil
                   business-time
                   transact-time)]
        ;; (prn :aev e v)
        (doc/update-binary-join-order! binary-idx e-doc-idx v-idx)))))

(defn- bgp-joins [snapshot {:keys [object-store business-time transact-time] :as db} bgp-clauses var->joins]
  (->> bgp-clauses
       (reduce
        (fn [[deps var->joins] {:keys [e a v] :as clause}]
          (let [e-var e
                v-var (if (logic-var? v)
                        v
                        (gensym "literal-value"))
                binary-idx (with-meta (doc/new-binary-join-virtual-index) {:clause clause
                                                                           :names {:e e-var
                                                                                   :v v-var}})
                indexes (if (= e v)
                          (throw (UnsupportedOperationException. (str "Self join not currently supported: " (pr-str clause))))
                          {v-var [(assoc binary-idx :name e-var)]
                           e-var [(assoc binary-idx :name e-var)]})
                indexes (if (literal? e)
                          (merge-with into indexes {e-var [(doc/new-relation-virtual-index e-var [[e]] 1)]})
                          indexes)
                indexes (if (literal? v)
                          (merge-with into indexes {v-var [(doc/new-relation-virtual-index v-var [[v]] 1)]})
                          indexes)]
            [(cond-> deps
               (literal? e)
               (assoc v-var e-var)
               (and (literal? v)
                    (not (literal? e)))
               (assoc e-var v-var))
             (merge-with into var->joins indexes)]))
        [{} var->joins])))

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
    [(->> arg-vars
          (reduce
           (fn [var->joins arg-var]
             (->> {arg-var
                   (cond-> [(assoc relation :name (symbol "crux.query.value" (name arg-var)))]
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
           var->joins))
     relation]))

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

;; TODO: This should in theory not be necessary, it's an artefact of
;; how there can be several entities with the same value returned, but
;; only some of them might match a predicate. As the results currently
;; is a map of variables to sets of values, there's no way to
;; constrain across two variables. This used to work when there were
;; leaf predicates, which did fire on each actual row at the end.
;; Note that the sub value groups use variable names as keys, not
;; result names.
(defn- valid-sub-tuple? [join-results bound-result-tuple]
  (let [{:keys [valid-sub-value-groups]} (meta join-results)]
    (if (empty? valid-sub-value-groups)
      true
      (let [tuple (zipmap (map :var bound-result-tuple)
                          (map :value bound-result-tuple))]
        (->> (for [valid-sub-value-group valid-sub-value-groups]
               (some #(let [ks (set/intersection (set (keys %))
                                                 (set (keys tuple)))]
                        (= (select-keys % ks) (select-keys tuple ks)))
                     valid-sub-value-group))
             (every? true?))))))

(defn- bound-result->join-result [{:keys [result-name value? type entity value] :as result}]
  (if value?
    {result-name #{value}}
    {result-name #{entity}}))

(defn- bound-results-for-var [object-store var->bindings join-keys join-results var]
  (let [{:keys [e-var var attr result-index result-name type]} (get var->bindings var)]
    (if (and (= "crux.query.value" (namespace result-name))
             (contains? join-results result-name))
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
              needs-valid-sub-value-group? (> (count (distinct (filter logic-var? args))) 1)
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)]]
    (do (doseq [var pred-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Predicate refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn [join-keys join-results]
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
                  pred-result+result-maps+args-tuple
                  (for [bound-result-args-tuple bound-result-args-tuples
                        :when (valid-sub-tuple? join-results bound-result-args-tuple)
                        pred-fn pred-fns
                        :let [pred-result (apply pred-fn (mapv :value bound-result-args-tuple))]
                        :when pred-result]
                    [pred-result
                     (->> (for [{:keys [literal-arg?] :as result} bound-result-args-tuple
                                :when (not literal-arg?)]
                            (bound-result->join-result result))
                          (apply merge-with into))
                     (when needs-valid-sub-value-group?
                       (->> (for [[arg {:keys [value literal-arg?]}] (map vector args bound-result-args-tuple)
                                  :when (not literal-arg?)]
                              [arg value])
                            (into {})))])]
              (when return
                (->> (for [[pred-result] pred-result+result-maps+args-tuple]
                       [pred-result])
                     (distinct)
                     (vec)
                     (doc/update-relation-virtual-index! relation)))
              (when-let [join-results (some->> (map second pred-result+result-maps+args-tuple)
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

(defn- or-single-e-var-bgp-fast-path [snapshot {:keys [object-store business-time transact-time] :as db} where args]
  (let [[[_ {:keys [e a v] :as clause}]] where
        entities (mapv e args)
        idx (doc/new-shared-literal-attribute-for-known-entities-virtual-index
             object-store snapshot entities [[a v]] business-time transact-time)
        idx-as-seq (doc/idx->seq idx)]
    (when (seq idx-as-seq)
      [[[]
        {e (set (for [[_ entities] idx-as-seq
                      entity entities]
                  entity))}]])))

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
        (fn [join-keys join-results]
          (if (= (count join-keys) or-join-depth)
            (let [args (vec (for [bound-result-tuple (cartesian-product
                                                      (for [var bound-vars]
                                                        (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                  :when (valid-sub-tuple? join-results bound-result-tuple)]
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
                                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                                              :when (valid-sub-tuple? join-results bound-result-tuple)]
                                                          (mapv :value bound-result-tuple)))
                                                   (->> (for [bound-result-tuple (cartesian-product
                                                                                  (for [var bound-vars]
                                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                                              :when (valid-sub-tuple? join-results bound-result-tuple)
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

(defn- build-unification-preds [snapshot {:keys [object-store] :as db} unify-clauses var->bindings]
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
                          (let [{:keys [result-index]} (get var->bindings arg)]
                            (or (some->> (get join-keys result-index)
                                         (sorted-set-by bu/bytes-comparator))
                                (some->> (bound-results-for-var object-store var->bindings join-keys join-results arg)
                                         (map (comp idx/value->bytes :value))
                                         (into (sorted-set-by bu/bytes-comparator)))))
                          (->> (map idx/value->bytes (doc/normalize-value arg))
                               (into (sorted-set-by bu/bytes-comparator)))))]
            (if (and x y)
              (case op
                == (boolean (not-empty (set/intersection x y)))
                != (empty? (set/intersection x y)))
              true))))))

(defn- build-not-constraints [snapshot {:keys [object-store] :as db} rule-name->rules not-type not-clauses var->bindings]
  (for [not-clause not-clauses
        :let [[not-vars not-clause] (case not-type
                                      :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                            not-clause]
                                      :not-join [(:args not-clause)
                                                 (:body not-clause)])
              not-vars (remove blank-var? not-vars)
              not-join-depth (calculate-constraint-join-depth var->bindings not-vars)
              needs-valid-sub-value-group? (> (count not-vars) 1)]]
    (do (doseq [arg not-vars
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Not refers to unknown variable: "
                       arg " " (pr-str not-clause)))))
        (fn [join-keys join-results]
          (if (= (count join-keys) not-join-depth)
            (with-open [snapshot (doc/new-cached-snapshot snapshot false)]
              (let [args (vec (for [bound-result-tuple (cartesian-product
                                                        (for [var not-vars]
                                                          (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                    :when (valid-sub-tuple? join-results bound-result-tuple)
                                    :let [args-tuple (zipmap not-vars (map :value bound-result-tuple))]]
                                (with-meta args-tuple {:bound-result-tuple bound-result-tuple})))
                    {:keys [n-ary-join
                            var->bindings]} (build-sub-query snapshot db not-clause args rule-name->rules)
                    args-to-remove (set (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join)
                                              bound-result-tuple (cartesian-product
                                                                  (for [var not-vars]
                                                                    (bound-results-for-var object-store var->bindings join-keys join-results var)))
                                              :when (valid-sub-tuple? join-results bound-result-tuple)]
                                          (zipmap not-vars (map :value bound-result-tuple))))
                    args-to-keep (remove args-to-remove args)]
                (when-let [join-results (some->> (for [args args-to-keep
                                                       :let [{:keys [bound-result-tuple]} (meta args)]
                                                       result bound-result-tuple]
                                                   (bound-result->join-result result))
                                                 (apply merge-with into)
                                                 (merge join-results))]
                  (if needs-valid-sub-value-group?
                    (vary-meta join-results update :valid-sub-value-groups conj (set args-to-keep))
                    join-results))))
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

;; TODO: This is potentially simplistic. The attempt to split the vars
;; into two groups via non-leaf-v-vars needs thought, as this might
;; wreck dependency order. The intent is to push these joins to the
;; end, its done for performance reasons.
(defn- calculate-join-order [pred-clauses or-clause+relation+or-branches var->joins non-leaf-v-vars v-var->e arg-vars deps]
  (let [preds (for [{:keys [pred return] :as pred-clause} pred-clauses]
                [(filter logic-var? (:args pred))
                 (if return
                   [return]
                   [])])
        ors (for [[_ _ [{:keys [free-vars bound-vars]}]] or-clause+relation+or-branches]
              [bound-vars free-vars])
        g (->> (keys var->joins)
               (reduce
                (fn [g v]
                  (if (contains? non-leaf-v-vars v)
                    (dep/depend g v ::leaf)
                    (dep/depend g ::leaf v)))
                (dep/graph)))
        g (->> deps
               (reduce-kv
                (fn [g k v]
                  (dep/depend g k v))
                g))
        g (->> (concat preds ors)
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
        join-order (dep/topo-sort g)
        [leaves non-leaves] (->> join-order
                                 (partition-by #{::leaf})
                                 (remove #{[::leaf]}))
        can-reorder? (->> (for [leaf leaves]
                            (->> (dep/immediate-dependents g leaf)
                                 (filter var->joins)
                                 (empty?)))
                          (every? true?))]
    (vec (filter var->joins (if can-reorder?
                              (concat non-leaves leaves)
                              join-order)))))

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
                                                             :when (zero? (long (get-in recursion-cache cache-key 0)))
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
                     ;; TODO: Understand this, does this really work
                     ;; in the general case? Why less than equal one?
                     ;; This implies the rule can be expanded twice.
                     expanded-rules (if (<= (long (get-in recursion-cache cache-key 0)) 1)
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
                unification-vars
                pred-return-vars]} (collect-vars type->clauses)
        v-var->e (->> (for [{:keys [e v] :as clause} bgp-clauses
                            :when (logic-var? v)]
                        [v e])
                      (into {}))
        e->v-var (set/map-invert v-var->e)
        arg-vars (arg-vars args)
        var->joins {}
        v-var->range-constriants (build-v-var-range-constraints e-vars range-clauses)
        [deps var->joins] (bgp-joins snapshot
                                     db
                                     bgp-clauses
                                     var->joins)
        [var->joins args-relation] (arg-joins snapshot
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
        non-leaf-v-vars (set/union unification-vars e-vars)
        vars-in-join-order (calculate-join-order pred-clauses or-clause+relation+or-branches var->joins non-leaf-v-vars v-var->e arg-vars deps)
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
        unification-preds (vec (build-unification-preds snapshot db unify-clauses var->bindings))
        not-constraints (vec (concat (build-not-constraints snapshot db rule-name->rules :not not-clauses var->bindings)
                                     (build-not-constraints snapshot db rule-name->rules :not-join not-join-clauses var->bindings)))
        pred-constraints (vec (build-pred-constraints object-store pred-clause+relations var->bindings))
        or-constraints (vec (build-or-constraints snapshot db rule-name->rules or-clause+relation+or-branches
                                                  var->bindings vars-in-join-order v-var->range-constriants))
        shared-e-v-vars (set/intersection e-vars v-vars)
        constrain-result-fn (fn [max-ks result]
                              (some->> (constrain-join-result-by-join-keys var->bindings shared-e-v-vars max-ks result)
                                       (doc/constrain-join-result-by-empty-names max-ks)
                                       (constrain-join-result-by-unification unification-preds max-ks)
                                       (constrain-join-result-by-constraints pred-constraints max-ks)
                                       (constrain-join-result-by-constraints not-constraints max-ks)
                                       (constrain-join-result-by-constraints or-constraints max-ks)
                                       (doc/constrain-join-result-by-empty-names max-ks)))
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
    ;; (prn vars-in-join-order)
    ;; (prn var->bindings)
    ;; (prn (zipmap (keys var->joins) (map #(map type %) (vals var->joins))))
    (constrain-result-fn [] [])
    {:n-ary-join (-> (mapv doc/new-unary-join-virtual-index joins)
                     (doc/new-n-ary-join-layered-virtual-index)
                     (doc/new-n-ary-constraining-layered-virtual-index constrain-result-fn))
     :var->bindings var->bindings}))

(defn q
  ([{:keys [kv] :as db} q]
   (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot kv) true)]
     (set (crux.query/q snapshot db q))))
  ([snapshot {:keys [object-store] :as db} q]
   (let [{:keys [find where args rules] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (IllegalArgumentException.
               (str "Invalid input: " (s/explain-str :crux.query/query q)))))
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
                                   (bound-results-for-var object-store var->bindings join-keys join-results var)))
             :when (valid-sub-tuple? join-results bound-result-tuple)]
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
