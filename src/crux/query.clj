(ns crux.query
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
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

(def ^:private built-ins '#{not == !=})

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of logic-var? :kind vector? :min-count 1))

(s/def ::bgp (s/and vector? (s/cat :e (some-fn logic-var? entity-ident?)
                                   :a keyword?
                                   :v (s/? any?))))
(s/def ::not-bgp (s/and ::bgp (comp logic-var? :e)))
(s/def ::or-bgp (s/and ::bgp (comp logic-var? :e) (comp literal? :v)))
(s/def ::and (expression-spec 'and (s/+ ::or-bgp)))

(s/def ::pred (s/and list?
                     (s/cat :pred-fn ::pred-fn
                            :args (s/* any?))))

(s/def ::range-op '#{< <= >= >})

(s/def ::term (s/or :bgp ::bgp
                    :not (expression-spec 'not (s/& ::not-bgp))
                    :or (expression-spec 'or (s/+ (s/or :bgp ::or-bgp
                                                        :and ::and)))
                    :range (s/or :sym-val (s/cat :op ::range-op
                                                 :sym logic-var?
                                                 :val literal?)
                                 :val-sym (s/cat :op ::range-op
                                                 :val literal?
                                                 :sym logic-var?))
                    :unify (s/cat :op '#{== !=}
                                  :x any?
                                  :y any?)
                    :pred (s/or :pred ::pred
                                :not-pred (expression-spec 'not (s/& ::pred)))))

(s/def ::arg-tuple (s/map-of (some-fn logic-var? keyword?) any?))
(s/def ::args (s/coll-of ::arg-tuple :kind vector?))

(s/def ::where (s/coll-of ::term :kind vector? :min-count 1))
(s/def ::query (s/keys :req-un [::find ::where] :opt-un [::args]))

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

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         {type [(case type
                  (:bgp, :not) (normalize-bgp-clause clause)
                  :pred (let [[type {:keys [pred-fn args]
                                     :as clause}] clause]
                          (let [clause (if-let [range-pred (and (= 2 (count args))
                                                                (every? logic-var? args)
                                                                (get pred->built-in-range-pred pred-fn))]
                                         (assoc clause :pred-fn range-pred)
                                         clause)]
                            (case type
                              :pred clause
                              :not-pred (update clause :pred-fn complement))))
                  :or (for [[type clause] clause]
                        (case type
                          :bgp [(normalize-bgp-clause clause)]
                          :and (mapv normalize-bgp-clause clause)))
                  :range (let [[type clause] clause]
                           (if (= :val-sym type)
                             (update clause :op range->inverse-range)
                             clause))
                  clause)]})
       (apply merge-with into)))

(defn- collect-vars [{bgp-clauses :bgp
                      unify-clauses :unify
                      not-clauses :not
                      or-clauses :or
                      pred-clauses :pred}]
  (let [or-e-vars (set (for [or-clause or-clauses
                             sub-clause or-clause
                             {:keys [e]} sub-clause]
                         e))
        not-v-vars (set (for [{:keys [v]} not-clauses
                              :when (logic-var? v)]
                          v))]
    {:e-vars (->> (for [{:keys [e]} bgp-clauses
                        :when (logic-var? e)]
                    e)
                  (into or-e-vars))
     :v-vars (->> (for [{:keys [v]} bgp-clauses
                        :when (logic-var? v)]
                    v)
                  (into not-v-vars))
     :unification-vars (set (for [{:keys [x y]} unify-clauses
                                  arg [x y]
                                  :when (logic-var? arg)]
                              arg))
     :not-vars (->> (for [{:keys [e]} not-clauses]
                      e)
                    (into not-v-vars))
     :pred-vars (set (for [{:keys [args]} pred-clauses
                           arg args
                           :when (logic-var? arg)]
                       arg))}))

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

(defn- e-var-literal-v-or-joins [snapshot or-clauses var->joins business-time transact-time]
  (->> or-clauses
       (reduce
        (fn [var->joins clause]
          (let [or-e-vars (set (for [sub-clauses clause
                                     {:keys [e]} sub-clauses]
                                 e))
                e-var (first or-e-vars)]
            (when (not= 1 (count or-e-vars))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variable in entity position: "
                           (pr-str clause)))))
            (let [idx (doc/new-or-virtual-index
                       (vec (for [sub-clauses clause]
                              (doc/new-shared-literal-attribute-entities-virtual-index
                               snapshot
                               (vec (for [{:keys [a v]
                                           :as clause} sub-clauses]
                                      [a v]))
                               business-time
                               transact-time))))]
              (merge-with into var->joins {e-var [(assoc idx :name e-var)]}))))
        var->joins)))

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
        (throw (IllegalArgumentException. (str "Argument maps need to contain the same keys as first map: " ks " " (keys m))))))
    (set (for [k ks]
           (symbol (name k))))))

(defn- arg-joins [snapshot args e-vars var->joins business-time transact-time]
  (let [arg-keys-in-join-order (sort (keys (first args)))
        relation (doc/new-relation-virtual-index (gensym "args")
                                                 (for [arg args]
                                                   (mapv arg arg-keys-in-join-order))
                                                 (count arg-keys-in-join-order))
        arg-vars (arg-vars args)]
    (doseq [arg-var arg-vars]
      (when (and (not (contains? var->joins arg-var))
                 (not (contains? e-vars arg-var)))
        (throw (IllegalArgumentException. (str "Argument refers to unknown variable: " arg-var)))))
    (->> arg-vars
         (reduce
          (fn [var->joins arg-var]
            (->> (merge
                  {arg-var
                   (cond-> [relation]
                     (and (not (contains? var->joins arg-var))
                          (contains? e-vars arg-var))
                     (conj (assoc (doc/new-entity-attribute-value-virtual-index
                                   snapshot
                                   :crux.db/id
                                   nil
                                   business-time
                                   transact-time)
                                  :name arg-var)))})
                 (merge-with into var->joins)))
          var->joins))))

(defn- build-var-bindings [var->attr v-var->e var->values-result-index e-var->leaf-v-var-clauses vars]
  (->> (for [var vars
             :let [e (get v-var->e var var)]]
         [var {:e-var e
               :var var
               :attr (get var->attr var)
               :result-index (get var->values-result-index var)
               :required-attrs (some->> (get e-var->leaf-v-var-clauses e)
                                        (not-empty)
                                        (map :a)
                                        (set))}])
       (into {})))

(defn- bound-results-for-var [object-store var->bindings join-keys join-results var]
  (let [{:keys [e-var var attr result-index required-attrs]} (get var->bindings var)
        entities (get join-results e-var)
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
       :entity entity})))

(defn- build-pred-constraints [object-store pred-clauses var->bindings]
  (for [{:keys [pred-fn args]
         :as clause} pred-clauses
        :let [pred-vars (filter logic-var? args)
              pred-join-depths (for [var pred-vars]
                                 (get-in var->bindings [var :result-index]))]
        :when (not-any? nil? pred-join-depths)
        :let [pred-join-depth (inc (apply max pred-join-depths))]]
    (do (doseq [var pred-vars
                :when (not (contains? var->bindings var))]
          (throw (IllegalArgumentException.
                  (str "Predicate refers to unknown variable: "
                       var " " (pr-str clause)))))
        (fn [join-keys join-results]
          (if (= (count join-keys) pred-join-depth)
            (when (->> (for [arg args]
                         (if (logic-var? arg)
                           (->> (bound-results-for-var object-store var->bindings join-keys join-results arg)
                                (first)
                                :value)
                           arg))
                       (apply pred-fn))
              join-results)
            join-results)))))

(defn- build-leaf-pred [pred-clauses var->bindings]
  (some->> (for [{:keys [pred-fn args]
                  :as clause} pred-clauses
                 :let [pred-vars (filter logic-var? args)
                       pred-join-depths (for [var pred-vars]
                                          (get-in var->bindings [var :result-index]))]
                 :when (some nil? pred-join-depths)]
             (do (doseq [var pred-vars
                         :when (not (contains? var->bindings var))]
                   (throw (IllegalArgumentException.
                           (str "Predicate refers to unknown variable: "
                                var " " (pr-str clause)))))
                 (fn [var->result]
                   (->> (for [arg args]
                          (if (logic-var? arg)
                            (get var->result arg)
                            arg))
                        (apply pred-fn)))))
           (not-empty)
           (apply every-pred)))

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

(defn- build-not-constraints [object-store not-clauses var->bindings]
  (for [{:keys [e a v]
         :as clause} not-clauses]
    (do (doseq [arg [e v]
                :when (and (logic-var? arg)
                           (not (contains? var->bindings arg)))]
          (throw (IllegalArgumentException.
                  (str "Not refers to unknown variable: "
                       arg " " (pr-str clause)))))
        (fn [join-keys join-results]
          (let [results (bound-results-for-var object-store var->bindings join-keys join-results e)
                not-vs (if (logic-var? v)
                         (->> (bound-results-for-var object-store var->bindings join-keys join-results v)
                              (map :value)
                              (set))
                         #{v})
                entities-to-remove (set (for [{:keys [doc entity]} results
                                              :when (not-empty (set/intersection (set (doc/normalize-value (get doc a))) not-vs))]
                                          entity))]
            (update join-results e set/difference entities-to-remove))))))

(defn- constrain-join-result-by-unification [unification-preds join-keys join-results]
  (when (->> (for [pred unification-preds]
               (pred join-keys join-results))
             (every? true?))
    join-results))

(defn- constrain-join-result-by-not [not-constraints var->joins join-keys join-results]
  (if (= (count join-keys) (count var->joins))
    (reduce
     (fn [results not-constraint]
       (not-constraint join-keys results))
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
  (when (->> (for [e-var shared-e-v-vars
                   :let [eid-bytes (get join-keys (get-in var->bindings [e-var :result-index]))]
                   :when eid-bytes
                   entity (get join-results e-var)]
               (bu/bytes=? eid-bytes (idx/id->bytes entity)))
             (every? true?))
    join-results))

(defn q
  ([{:keys [kv] :as db} q]
   (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot kv) true)]
     (set (crux.query/q snapshot db q))))
  ([snapshot {:keys [kv object-store business-time transact-time] :as db} q]
   (let [{:keys [find where args] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (IllegalArgumentException.
               (str "Invalid input: " (s/explain-str :crux.query/query q)))))
     (let [{bgp-clauses :bgp
            range-clauses :range
            pred-clauses :pred
            unify-clauses :unify
            not-clauses :not
            or-clauses :or
            :as type->clauses} (normalize-clauses where)
           {:keys [e-vars
                   v-vars
                   unification-vars
                   not-vars
                   pred-vars]} (collect-vars type->clauses)
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
           var->joins (e-var-literal-v-joins snapshot
                                             e-var->literal-v-clauses
                                             var->joins
                                             business-time
                                             transact-time)
           var->joins (e-var-literal-v-or-joins snapshot
                                                or-clauses
                                                var->joins
                                                business-time
                                                transact-time)
           non-leaf-v-vars (set/union unification-vars not-vars e-vars (arg-vars args))
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
           v-var->attr (->> (for [{:keys [e a v]} bgp-clauses
                                  :when (and (logic-var? v)
                                             (= e (get v-var->e v)))]
                              [v a])
                            (into {}))
           e-var->attr (zipmap e-vars (repeat :crux.db/id))
           var->attr (merge v-var->attr e-var->attr)
           var->values-result-index (zipmap (keys var->joins) (range))
           var->bindings (build-var-bindings var->attr
                                             v-var->e
                                             var->values-result-index
                                             e-var->leaf-v-var-clauses
                                             (keys var->attr))
           unification-preds (build-unification-preds unify-clauses var->bindings)
           not-constraints (build-not-constraints object-store not-clauses var->bindings)
           pred-constraints (build-pred-constraints object-store pred-clauses var->bindings)
           shared-e-v-vars (set/intersection e-vars v-vars)
           constrain-result-fn (fn [max-ks result]
                                 (some->> (doc/constrain-join-result-by-empty-names max-ks result)
                                          (constrain-join-result-by-join-keys var->bindings shared-e-v-vars max-ks)
                                          (constrain-join-result-by-unification unification-preds max-ks)
                                          (constrain-join-result-by-not not-constraints var->joins max-ks)
                                          (constrain-join-result-by-preds pred-constraints max-ks)))
           leaf-pred (build-leaf-pred pred-clauses var->bindings)
           all-vars (distinct (concat find pred-vars))]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException. (str "Find refers to unknown variable: " var))))
       (for [[join-keys join-results] (-> (mapv doc/new-unary-join-virtual-index (vals var->joins))
                                          (doc/new-n-ary-join-layered-virtual-index)
                                          (doc/layered-idx->seq (count var->joins) constrain-result-fn))
             result (cartesian-product
                     (for [var all-vars]
                       (bound-results-for-var object-store var->bindings join-keys join-results var)))
             :when (or (nil? leaf-pred)
                       (leaf-pred (zipmap all-vars (map :value result))))
             :let [find-result (take (count find) result)]]
         (with-meta
           (mapv :value find-result)
           (zipmap (map :e-var find-result) find-result)))))))

(defrecord QueryDatasource [kv object-store business-time transact-time])

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (doc/read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException. (str "Timed out waiting for: " transact-time
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
