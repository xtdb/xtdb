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
(def ^:private entity-ident? keyword?)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next)
         spec))

(def ^:private built-ins '#{and == !=})

(s/def ::bgp (s/and vector? (s/cat :e (some-fn logic-var? entity-ident?)
                                   :a keyword?
                                   :v (s/? any?))))

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
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

(defn- normalize-bgp-clause [clause]
  (if (nil? (:v clause))
    (assoc clause :v (gensym "_"))
    clause))

(def ^:private pred->built-in-range-pred {< (comp neg? compare)
                                          <= (comp not pos? compare)
                                          > (comp pos? compare)
                                          >= (comp not neg? compare)})

(def ^:private range->inverse-range '{< >=
                                      <= >
                                      > <=
                                      >= <})

(defn- blank-var? [v]
  (re-find #"^_\d*$"(name v)))

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
    (->> {:e-vars (set (for [{:keys [e]} bgp-clauses
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
                                arg (cons return (:args pred))
                                :when (logic-var? arg)]
                            arg))
          :rule-vars (set/union (set (for [{:keys [args]} rule-clauses
                                           arg args
                                           :when (logic-var? arg)]
                                       arg))
                                or-join-vars)}
         (merge-with set/union or-vars))))

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

(defn- e-var-literal-v-joins [snapshot e-var->literal-v-clauses var->joins business-time transact-time]
  (->> e-var->literal-v-clauses
       (reduce
        (fn [var->joins [e-var clauses]]
          (let [idx (doc/new-shared-literal-attribute-entities-virtual-index
                     snapshot
                     (vec (for [{:keys [a v]} clauses]
                            [a v]))
                     business-time
                     transact-time)]
            (merge-with into var->joins {e-var [(assoc idx :name e-var)]})))
        var->joins)))

(declare build-sub-query)

;; TODO: Needs cleanup. How state is transferred and shared between
;; sub-query and parent query needs work. Cannot deal with predicates
;; or not expressions on their own as there's nothing to join on in
;; that branch. Also needs to pass back vars in join order up, and
;; parent needs to respect this relative order and merging it into its
;; own. This also needs to happen across branches, which might require
;; to have the exact same join order. One potential alternative is to
;; always put all predicate (and rule) return vars at the end, though
;; they still need to be reordered based on their dependencies.  Yet
;; another alternative is to execute the or as a sub-query, and then
;; directly bind only the or-join vars into a resulting relation.
;; This would also work for recursive rules, if rule expansion is
;; postponed, which it will be for recursive rules anyway. The
;; resulting relations can still us an n-ary or to wrap them up for
;; the parent query. As they would been realised, there should then be
;; no dependency between parent and sub-query join order.
(defn- or-joins [snapshot db rules or-type or-clauses var->joins]
  (->> or-clauses
       (reduce
        (fn [[or-var->bindings var->joins] clause]
          (let [or-join? (= :or-join or-type)
                or-join-args (when or-join?
                               (set (:args clause)))
                or-branches (for [[type sub-clauses] (case or-type
                                                       :or clause
                                                       :or-join (:body clause))
                                  :let [where (case type
                                                :term [sub-clauses]
                                                :and sub-clauses)
                                        local->hidden-var (when or-join?
                                                            (let [body-vars (->> (collect-vars (normalize-clauses where))
                                                                                 (vals)
                                                                                 (reduce into #{}))
                                                                  local-vars (set/difference body-vars or-join-args)]
                                                              (zipmap local-vars (map gensym local-vars))))]]
                              {:local-vars (vals local->hidden-var)
                               :where (if or-join?
                                        (w/postwalk-replace local->hidden-var where)
                                        where)
                               :sub-clauses sub-clauses})
                all-local-vars (->> (map :local-vars or-branches)
                                    (reduce into #{}))
                sub-query-state (->> (for [{:keys [local-vars where sub-clauses]} or-branches
                                           :let [other-local-vars (set/difference all-local-vars local-vars)
                                                 place-holder-args [(->> (for [var other-local-vars]
                                                                            [var true])
                                                                          (into {}))]
                                                 sub-query-state (build-sub-query snapshot
                                                                                  db
                                                                                  []
                                                                                  where
                                                                                  place-holder-args
                                                                                  rules)]]
                                       (assoc sub-query-state :sub-clauses sub-clauses))
                                     (reduce
                                      (fn [lhs rhs]
                                        (let [lhs-vars (set (keys (:var->bindings lhs)))
                                              rhs-vars (set (keys (:var->bindings rhs)))]
                                          (when (and (= :or or-type)
                                                     (not (= lhs-vars rhs-vars)))
                                            (throw (IllegalArgumentException.
                                                    (str "Or requires same logic variables: "
                                                         (pr-str (:sub-clauses lhs)) " "
                                                         (pr-str (:sub-clauses rhs)))))))
                                        (-> (merge-with merge lhs (select-keys rhs [:var->joins :var->bindings]))
                                            (update :local-hidden-vars set/union (:local-hidden-vars rhs))
                                            (update :n-ary-join doc/new-n-ary-or-layered-virtual-index (:n-ary-join rhs))))))
                idx (:n-ary-join sub-query-state)
                sub-query-var->joins (:var->joins sub-query-state)]
            [(merge or-var->bindings (:var->bindings sub-query-state))
             (apply merge-with into var->joins (for [v (case or-type
                                                         :or (keys (:var->bindings sub-query-state))
                                                         :or-join (concat all-local-vars or-join-args))
                                                     :when (contains? sub-query-var->joins v)]
                                                 {v [(assoc idx :name v)]}))]))
        [nil var->joins])))

(defn- e-var-v-var-joins [snapshot e-var+v-var->join-clauses v-var->range-constrants var->joins business-time transact-time]
  (->> e-var+v-var->join-clauses
       (reduce
        (fn [var->joins [[e-var v-var] clauses]]
          (let [indexes (for [{:keys [a]} clauses]
                          (assoc (doc/new-entity-attribute-value-virtual-index
                                  snapshot
                                  a
                                  (get v-var->range-constrants v-var)
                                  business-time
                                  transact-time)
                                 :name e-var))]
            (merge-with into var->joins {v-var (vec indexes)})))
        var->joins)))

(defn- v-var-literal-e-joins [snapshot object-store v-var->literal-e-clauses v-var->range-constrants var->joins business-time transact-time]
  (->> v-var->literal-e-clauses
       (reduce
        (fn [var->joins [v-var clauses]]
          (let [indexes (for [{:keys [e a]} clauses]
                          (assoc (doc/new-literal-entity-attribute-values-virtual-index
                                  object-store
                                  snapshot
                                  e
                                  a
                                  (get v-var->range-constrants v-var)
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

(defn- arg-joins [snapshot args e-vars var->joins business-time transact-time]
  (let [arg-keys-in-join-order (sort (keys (first args)))
        relation (doc/new-relation-virtual-index (gensym "args")
                                                 (for [arg args]
                                                   (mapv arg arg-keys-in-join-order))
                                                 (count arg-keys-in-join-order))]
    (->> (arg-vars args)
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

(defn- pred-joins [pred-clauses var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause->relation var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [relation (doc/new-relation-virtual-index (gensym "preds")
                                                           []
                                                           1)]
              [(assoc pred-clause->relation pred-clause relation)
               (->> {return
                     [(assoc relation :name (symbol "crux.query.pred" (name return)))]}
                    (merge-with into var->joins))])
            [pred-clause->relation var->joins]))
        [{} var->joins])))

(defn- build-var-bindings [var->attr v-var->e var->values-result-index e-var->leaf-v-var-clauses vars]
  (->> (for [var vars
             :let [e (get v-var->e var var)
                   leaf-var? (contains? e-var->leaf-v-var-clauses e)]]
         [var {:e-var e
               :var var
               :attr (get var->attr var)
               :result-index (get var->values-result-index var)
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
  (->> (for [var arg-vars]
         [var {:var var
               :result-name (symbol "crux.query.arg" (name var))
               :result-index (get var->values-result-index var)
               :type :arg}])
       (into {})))

(defn- build-pred-return-var-bindings [var->values-result-index pred-clauses]
  (->> (for [{:keys [return]} pred-clauses
             :when return]
         [return {:var return
                  :result-name (symbol "crux.query.pred" (name return))
                  :result-index (get var->values-result-index return)
                  :type :pred}])
       (into {})))

(defn- bound-results-for-var [object-store var->bindings join-keys join-results var]
  (let [{:keys [e-var var attr result-index result-name required-attrs type]} (get var->bindings var)
        bound-var? (and (= :entity-leaf type)
                        (not= e-var result-name)
                        (contains? join-results result-name))]
    (cond
      (= :arg type)
      (let [results (get join-results result-name)]
        (for [value results]
          {:value value
           :result-name result-name
           :type :arg
           :value? true}))

      (= :pred type)
      (let [results (get join-results result-name)]
        (for [value results]
          {:value value
           :result-name result-name
           :type :pred
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

(defn- build-pred-constraints [object-store pred-clauses var->bindings join-depth pred-clause->relation]
  (for [{:keys [pred return] :as clause} pred-clauses
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? args)
              pred-join-depths (for [var pred-vars]
                                 (or (get-in var->bindings [var :result-index])
                                     (dec join-depth)))
              pred-join-depth (inc (apply max -1 pred-join-depths))]]
    (do (doseq [var pred-vars]
          (when (not (contains? var->bindings var))
            (throw (IllegalArgumentException.
                    (str "Predicate refers to unknown variable: "
                         var " " (pr-str clause))))))
        (fn [join-keys join-results]
          (if (= (count join-keys) pred-join-depth)
            (let [pred-result+result-maps (for [args-to-apply (cartesian-product
                                                               (for [arg args]
                                                                 (if (logic-var? arg)
                                                                   (bound-results-for-var object-store var->bindings join-keys join-results arg)
                                                                   [{:value arg
                                                                     :literal-arg? true}])))
                                                :let [pred-result (apply pred-fn (map :value args-to-apply))]
                                                :when pred-result
                                                {:keys [result-name value entity value? literal-arg?] :as result} args-to-apply
                                                :when (not literal-arg?)]
                                            [pred-result {result-name #{(if value?
                                                                          value
                                                                          entity)}}])]
              (when return
                (doc/update-relation-virtual-index! (get pred-clause->relation clause)
                                                    (->> (for [[pred-result] pred-result+result-maps]
                                                           [pred-result])
                                                         (distinct)
                                                         (vec))))
              (some->> (map second pred-result+result-maps)
                       (apply merge-with into)
                       (not-empty)
                       (merge join-results)))
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

(defn- build-not-constraints [snapshot db object-store rules not-type not-clauses var->bindings]
  (for [not-clause not-clauses
        :let [[not-vars not-clause] (case not-type
                                      :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                            not-clause]
                                      :not-join [(:args not-clause)
                                                 (:body not-clause)])
              not-vars (remove blank-var? not-vars)]]
    (do (doseq [arg not-vars
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Not refers to unknown variable: "
                       arg " " (pr-str not-clause)))))
        (fn [join-keys join-results]
          (let [args (vec (for [tuple (cartesian-product
                                       (for [var not-vars]
                                         (bound-results-for-var object-store var->bindings join-keys join-results var)))]
                            (zipmap not-vars (map :value tuple))))
                parent-join-keys join-keys
                parent-var->bindings var->bindings
                {:keys [n-ary-join
                        var->bindings
                        var->joins]} (build-sub-query snapshot db [] not-clause args rules)]
            (->> (doc/layered-idx->seq n-ary-join (count var->joins))
                 (reduce
                  (fn [parent-join-results [join-keys join-results]]
                    (let [results (for [var not-vars]
                                    (bound-results-for-var object-store var->bindings join-keys join-results var))
                          result-types (for [result results
                                             {:keys [type]} result]
                                         type)]
                      (when (not-any? #{:arg} result-types)
                        (let [not-var->values (zipmap not-vars
                                                      (for [result results]
                                                        (->> result
                                                             (map :value)
                                                             (set))))
                              entities-to-remove (->> (for [[var not-vs] not-var->values
                                                            :let [parent-results (bound-results-for-var object-store parent-var->bindings
                                                                                                        parent-join-keys parent-join-results var)]
                                                            {:keys [e-var value entity]} parent-results
                                                            :when (contains? not-vs value)]
                                                        {e-var #{entity}})
                                                      (apply merge-with into))]
                          (merge-with set/difference parent-join-results entities-to-remove)))))
                  join-results)))))))

(defn- constrain-join-result-by-unification [unification-preds join-keys join-results]
  (when (->> (for [pred unification-preds]
               (pred join-keys join-results))
             (every? true?))
    join-results))

(defn- constrain-join-result-by-not [not-constraints join-depth join-keys join-results]
  (if (= (count join-keys) join-depth)
    (reduce
     (fn [results not-constraint]
       (when results
         (not-constraint join-keys results)))
     join-results
     not-constraints)
    join-results))

(defn- constrain-join-result-by-preds [pred-constraints join-keys join-results]
  (reduce
   (fn [results pred-constraint]
     (when results
       (pred-constraint join-keys results)))
   join-results
   pred-constraints))

(defn- constrain-join-result-by-join-keys [var->bindings shared-e-v-vars join-keys join-results]
  (->> (for [e-var shared-e-v-vars
             :let [eid-bytes (get join-keys (get-in var->bindings [e-var :result-index]))]
             :when eid-bytes
             entity (get join-results e-var)
             :when (not (bu/bytes=? eid-bytes (idx/id->bytes entity)))]
         {e-var #{entity}})
       (apply merge-with set/difference join-results)))

(defn- calculate-join-order [pred-clauses var->joins]
  (let [vars-in-join-order (vec (keys var->joins))
        var->index (zipmap vars-in-join-order (range))]
    (->> pred-clauses
         (reduce
          (fn [[vars-in-join-order seen-returns var->index] {:keys [pred return] :as pred-clause}]
            (if return
              (let [pred-args (filter logic-var? (:args pred))
                    max-dependent-var-index (->> (map var->index pred-args)
                                                 (reduce max -1))
                    return-index (get var->index return)
                    seen-returns (conj seen-returns return)]
                (when (some seen-returns pred-args)
                  (throw (IllegalArgumentException.
                          (str "Predicate has circular dependency: " (pr-str pred-clause)))))
                (if (> max-dependent-var-index return-index)
                  (let [dependent-var (get vars-in-join-order max-dependent-var-index)
                        vars-in-join-order (-> vars-in-join-order
                                               (assoc return-index dependent-var)
                                               (assoc max-dependent-var-index return))]
                    [vars-in-join-order
                     seen-returns
                     (zipmap vars-in-join-order (range))])
                  [vars-in-join-order seen-returns var->index]))
              [vars-in-join-order seen-returns var->index]))
          [vars-in-join-order #{} var->index])
         (first))))

(defn- expand-rules [where rule-name->rules seen-rules]
  (->> (for [[type clause :as sub-clause] where]
         (if (= :rule type)
           (let [rule-name (:name clause)
                 rules (get rule-name->rules rule-name)]
             (when-not rules
               (throw (IllegalArgumentException.
                       (str "Unknown rule: " (pr-str sub-clause)))))
             (when (contains? seen-rules rule-name)
               (throw (UnsupportedOperationException.
                       (str "Cannot do recursive rules yet: " (pr-str sub-clause)))))
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
               (let [expanded-rules (for [[rule-args body] rule-args+body
                                          :let [rule-arg->query-arg (zipmap rule-args (:args clause))]]
                                      (expand-rules (w/postwalk-replace rule-arg->query-arg body)
                                                    rule-name->rules
                                                    (conj seen-rules rule-name)))]
                 (if (= (count rules) 1)
                   (first expanded-rules)
                   (do (doseq [expanded-rule expanded-rules
                               :let [clause-types (set (map first expanded-rule))]
                               :when (not (contains? clause-types :bgp))]
                         (throw (UnsupportedOperationException.
                                 (str "Cannot do or between rules without basic graph patterns: " (pr-str sub-clause)))))
                       [[:or-join
                         {:args (vec (filter logic-var? (:args clause)))
                          :body (vec (for [expanded-rule expanded-rules]
                                       [:and expanded-rule]))}]])))))
           [sub-clause]))
       (reduce into [])))

(defn- build-sub-query [snapshot {:keys [kv object-store business-time transact-time] :as db} find where args rules]
  (let [rule-name->rules (group-by (comp :name :head) rules)
        where (expand-rules where rule-name->rules #{})
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
                            {:keys [e v]} clauses
                            :when (not (contains? e-vars v))]
                        [v e])
                      (into {}))
        e-var->literal-v-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                            :when (and (logic-var? e)
                                                       (literal? v))]
                                        clause)
                                      (group-by :e))
        v-var->literal-e-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                            :when (and (entity-ident? e)
                                                       (logic-var? v))]
                                        clause)
                                      (group-by :v))
        var->joins (sorted-map)
        [or-var->bindings var->joins] (or-joins snapshot
                                                db
                                                rules
                                                :or
                                                or-clauses
                                                var->joins)
        [or-var->bindings var->joins] (or-joins snapshot
                                                db
                                                rules
                                                :or-join
                                                or-join-clauses
                                                var->joins)
        var->joins (e-var-literal-v-joins snapshot
                                          e-var->literal-v-clauses
                                          var->joins
                                          business-time
                                          transact-time)
        arg-vars (arg-vars args)
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
        v-var->range-constrants (build-v-var-range-constraints e-vars range-clauses)
        var->joins (e-var-v-var-joins snapshot
                                      e-var+v-var->join-clauses
                                      v-var->range-constrants
                                      var->joins
                                      business-time
                                      transact-time)
        var->joins (v-var-literal-e-joins snapshot
                                          object-store
                                          v-var->literal-e-clauses
                                          v-var->range-constrants
                                          var->joins
                                          business-time
                                          transact-time)
        var->joins (arg-joins snapshot
                              args
                              e-vars
                              var->joins
                              business-time
                              transact-time)
        [pred-clause->relation var->joins] (pred-joins pred-clauses var->joins)
        v-var->attr (->> (for [{:keys [e a v]} bgp-clauses
                               :when (and (logic-var? v)
                                          (= e (get v-var->e v)))]
                           [v a])
                         (into {}))
        e-var->attr (zipmap e-vars (repeat :crux.db/id))
        var->attr (merge v-var->attr e-var->attr)
        join-depth (count var->joins)
        vars-in-join-order (calculate-join-order pred-clauses var->joins)
        var->values-result-index (zipmap vars-in-join-order (range))
        var->bindings (merge (build-pred-return-var-bindings var->values-result-index pred-clauses)
                             (build-arg-var-bindings var->values-result-index arg-vars)
                             or-var->bindings
                             (build-var-bindings var->attr
                                                 v-var->e
                                                 var->values-result-index
                                                 e-var->leaf-v-var-clauses
                                                 (keys var->attr)))
        unification-preds (vec (build-unification-preds unify-clauses var->bindings))
        not-constraints (vec (concat (build-not-constraints snapshot db object-store rules :not not-clauses var->bindings)
                                     (build-not-constraints snapshot db object-store rules :not-join not-join-clauses var->bindings)))
        pred-constraints (vec (build-pred-constraints object-store pred-clauses var->bindings join-depth pred-clause->relation))
        shared-e-v-vars (set/intersection e-vars v-vars)
        constrain-result-fn (fn [max-ks result]
                              (some->> (doc/constrain-join-result-by-empty-names max-ks result)
                                       (constrain-join-result-by-join-keys var->bindings shared-e-v-vars max-ks)
                                       (constrain-join-result-by-unification unification-preds max-ks)
                                       (constrain-join-result-by-not not-constraints join-depth max-ks)
                                       (constrain-join-result-by-preds pred-constraints max-ks)))
        joins (map var->joins vars-in-join-order)]
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
     (let [{:keys [n-ary-join
                   var->bindings
                   var->joins]} (build-sub-query snapshot db find where args rules)]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Find refers to unknown variable: " var))))
       (for [[join-keys join-results] (doc/layered-idx->seq n-ary-join (count var->joins))
             result (cartesian-product
                     (for [var find]
                       (bound-results-for-var object-store var->bindings join-keys join-results var)))]
         (with-meta
           (mapv :value result)
           (zipmap (map :e-var result) result)))))))

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
