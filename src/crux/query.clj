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

(defn- logic-var? [x]
  (symbol? x))

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next)
         spec))

(def ^:private  built-ins '#{not == !=})

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of logic-var? :kind vector?))

(s/def ::bgp (s/and vector?
                    (s/cat :e (some-fn logic-var? keyword?)
                           :a keyword?
                           :v (s/? any?))))
(s/def ::or-bgp (s/and vector?
                       (s/cat :e logic-var?
                              :a keyword?
                              :v (complement logic-var?))))
(s/def ::not-bgp (s/and vector?
                        (s/cat :e logic-var?
                               :a keyword?
                               :v (s/? any?))))
(s/def ::and (expression-spec 'and (s/+ ::or-bgp)))

(s/def ::term (s/or :bgp ::bgp
                    :not (expression-spec 'not (s/& ::not-bgp))
                    :or (expression-spec 'or (s/+ (s/or :bgp ::or-bgp
                                                        :and ::and)))
                    :range (s/cat :op '#{< <= >= >}
                                  :sym logic-var?
                                  :val (complement logic-var?))
                    :unify (s/cat :op '#{== !=}
                                  :x any?
                                  :y any?)
                    :pred (s/and list?
                                 (s/cat :pred-fn ::pred-fn
                                        :args (s/* any?)))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- cartesian-product [[x & xs]]
  (when (seq x)
    (for [a x
          bs (or (cartesian-product xs) [[]])]
      (cons a bs))))

(defn- normalize-bgp-clause [clause]
  (if (nil? (:v clause))
    (assoc clause :v (gensym "_"))
    clause))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         {type [(case type
                  (:bgp, :not) (normalize-bgp-clause clause)
                  :or (for [[type clause] clause]
                        (case type
                          :bgp [(normalize-bgp-clause clause)]
                          :and (mapv normalize-bgp-clause clause)))
                  clause)]})
       (apply merge-with into)))

(defn- v-var->range-constraints [e-vars range-clauses]
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

(defn- e-var-literal-v-joins [snapshot e-var->literal-v-clauses var->joins var->names business-time transact-time]
  (->> e-var->literal-v-clauses
       (reduce
        (fn [[var->joins var->names] [e-var clauses]]
          (let [var-name (gensym e-var)
                idx (doc/new-shared-literal-attribute-entities-virtual-index
                     snapshot
                     (vec (for [{:keys [a v]} clauses]
                            [a v]))
                     business-time transact-time)]
            [(merge-with into {e-var [(assoc idx :name var-name)]} var->joins)
             (merge-with into {e-var [var-name]} var->names)]))
        [var->joins var->names])))

(defn- e-var-literal-v-or-joins [snapshot or-clauses var->joins var->names business-time transact-time]
  (->> or-clauses
       (reduce
        (fn [[var->joins var->names] clause]
          (let [or-e-vars (set (for [sub-clauses clause
                                     {:keys [e]} sub-clauses]
                                 e))
                e-var (first or-e-vars)]
            (when (not= 1 (count or-e-vars))
              (throw (IllegalArgumentException.
                      (str "Or clause requires same logic variable in entity position: "
                           (pr-str clause)))))
            (let [var-name (gensym e-var)
                  idx (doc/new-or-virtual-index
                       (vec (for [sub-clauses clause]
                              (doc/new-shared-literal-attribute-entities-virtual-index
                               snapshot
                               (vec (for [{:keys [a v]
                                           :as clause} sub-clauses]
                                      [a v]))
                               business-time transact-time))))]
              [(merge-with into {e-var [(assoc idx :name var-name)]} var->joins)
               (merge-with into {e-var [var-name]} var->names)])))
        [var->joins var->names])))

(defn- e-var-v-var-joins [snapshot e-var+v-var->join-clauses v-var->range-constrants var->joins var->names business-time transact-time]
  (->> e-var+v-var->join-clauses
       (reduce
        (fn [[var->joins var->names] [[e-var v-var] clauses]]
          (let [indexes (for [{:keys [a]} clauses]
                          (assoc (doc/new-entity-attribute-value-virtual-index
                                  snapshot
                                  a
                                  (get v-var->range-constrants v-var)
                                  business-time
                                  transact-time)
                                 :name (gensym e-var)))]
            [(merge-with into {v-var (vec indexes)} var->joins)
             (merge-with into {e-var (mapv :name indexes)} var->names)]))
        [var->joins var->names])))

(defn- v-var-literal-e-joins [snapshot object-store v-var->literal-e-clauses v-var->range-constrants var->joins var->names business-time transact-time]
  (->> v-var->literal-e-clauses
       (reduce
        (fn [[var->joins var->names] [v-var clauses]]
          (let [indexes (for [{:keys [e a]} clauses]
                          (assoc (doc/new-literal-entity-attribute-values-virtual-index
                                  object-store
                                  snapshot
                                  e
                                  a
                                  (get v-var->range-constrants v-var)
                                  business-time transact-time)
                                 :name (gensym v-var)))]
            [(merge-with into {v-var (vec indexes)} var->joins)
             (merge {v-var (mapv :name indexes)} var->names)]))
        [var->joins var->names])))

(defn- results-for-var [object-store e-var->leaf-v-var-clauses var->names var-names->attr v-var->e-var join-results var]
  (let [constrain-doc-by-needed-attributes (fn [doc var]
                                             (->> (for [{:keys [a]} (get e-var->leaf-v-var-clauses var)]
                                                    (contains? doc a))
                                                  (every? true?)))
        var-name (first (get var->names var))
        attr (get var-names->attr var-name)
        e-var (get v-var->e-var var)
        result-var-name (if e-var
                          (first (get var->names e-var))
                          var-name)
        entities (get join-results result-var-name)
        content-hash->doc (->> (map :content-hash entities)
                               (db/get-objects object-store))]
    (for [[entity [_ doc]] (map vector entities content-hash->doc)
          :when (constrain-doc-by-needed-attributes doc (or e-var var))
          value (doc/normalize-value (get doc attr))]
      {:value value
       :var var
       :attr attr
       :doc doc
       :entity entity})))

(defn- build-preds [pred-clauses]
  (some->> (for [{:keys [pred-fn args]} pred-clauses]
             (fn [var->result]
               (->> (for [arg args]
                      (get var->result arg arg))
                    (apply pred-fn))))
           (not-empty)
           (apply every-pred)))

(defn- build-unification-preds [unify-clauses var->names var->v-result-index]
  (for [{:keys [op x y]
         :as clause} unify-clauses]
    (do (doseq [arg [x y]
                :when (and (logic-var? arg)
                           (not (contains? var->names arg)))]
          (throw (IllegalArgumentException.
                  (str "Unification refers to unknown variable: "
                       arg " " (pr-str clause)))))
        (fn [join-keys join-results]
          (let [[x y] (for [var [x y]]
                        (if (logic-var? var)
                          (or (some->> (get join-keys (get var->v-result-index var))
                                       (sorted-set-by bu/bytes-comparator))
                              (let [var-name (first (get var->names var))]
                                (some->> (get join-results var-name)
                                         (map (comp idx/id->bytes :eid))
                                         (into (sorted-set-by bu/bytes-comparator)))))
                          (->> (map idx/value->bytes (doc/normalize-value var))
                               (into (sorted-set-by bu/bytes-comparator)))))]
            (if (and x y)
              (case op
                == (boolean (not-empty (set/intersection x y)))
                != (empty? (set/intersection x y)))
              true))))))

(defn- build-not-constraints [object-store not-clauses e-var->leaf-v-var-clauses var->names var-names->attr v-var->e-var]
  (for [{:keys [e a v]
         :as clause} not-clauses]
    (do (doseq [arg [e v]
                :when (and (logic-var? arg)
                           (not (contains? var->names arg)))]
          (throw (IllegalArgumentException.
                  (str "Not refers to unknown variable: "
                       arg " " (pr-str clause)))))
        (fn [join-keys join-results]
          (let [results (results-for-var object-store e-var->leaf-v-var-clauses var->names var-names->attr v-var->e-var join-results e)
                vs (->> (if (logic-var? v)
                          (->> (results-for-var object-store e-var->leaf-v-var-clauses var->names var-names->attr v-var->e-var join-results v)
                               (map :value))
                          (doc/normalize-value v)))
                entities-to-remove (set (for [{:keys [doc entity]} results
                                              doc-v (doc/normalize-value (get doc a))
                                              v vs
                                              :when (= doc-v v)]
                                          entity))]
            (->> (get var->names e)
                 (reduce
                  (fn [result var-name]
                    (update result var-name set/difference entities-to-remove))
                  join-results)))))))

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

(defn- constrain-join-result-by-join-keys [var->v-result-index var->names shared-e-v-vars join-keys join-results]
  (when (->> (for [e-var shared-e-v-vars
                   :let [eid-bytes (get join-keys (get var->v-result-index e-var))]
                   :when eid-bytes
                   var-name (get var->names e-var)
                   entity (get join-results var-name)]
               (bu/bytes=? eid-bytes (idx/id->bytes entity)))
             (every? true?))
    join-results))

(defn q
  ([{:keys [kv] :as db} q]
   (with-open [snapshot (doc/new-cached-snapshot (ks/new-snapshot kv) true)]
     (set (crux.query/q snapshot db q))))
  ([snapshot {:keys [kv object-store business-time transact-time] :as db} q]
   (let [{:keys [find where] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (IllegalArgumentException.
               (str "Invalid input: " (s/explain-str :crux.query/query q)))))
     (let [{bgp-clauses :bgp
            range-clauses :range
            pred-clauses :pred
            unify-clauses :unify
            not-clauses :not
            or-clauses :or} (normalize-clauses where)
           e-vars (set (for [{:keys [e]} bgp-clauses
                             :when (logic-var? e)]
                         e))
           v-vars (set (for [{:keys [v]} bgp-clauses
                             :when (logic-var? v)]
                         v))
           unification-vars (set (for [{:keys [x y]
                                        :as clause} unify-clauses
                                       arg [x y]
                                       :when (logic-var? arg)]
                                   arg))
           not-vars (set (for [{:keys [e v]
                                :as clause} not-clauses
                               arg [e v]
                               :when (logic-var? arg)]
                           arg))
           or-vars (set (for [or-clause or-clauses
                              sub-clause or-clause
                              {:keys [e]
                               :as clause} sub-clause
                              :when (logic-var? e)]
                          e))
           e-vars (set/union e-vars or-vars)
           shared-e-v-vars (set/intersection e-vars v-vars)
           v-var->range-constrants (v-var->range-constraints e-vars range-clauses)
           e-var->v-var-clauses (->> (for [{:keys [e v]
                                            :as clause} bgp-clauses
                                           :when (and (logic-var? e)
                                                      (logic-var? v))]
                                       clause)
                                     (group-by :e))
           var->names (->> (for [[_ clauses] e-var->v-var-clauses
                                 {:keys [v]} clauses]
                             {v [(gensym v)]})
                           (apply merge-with into))
           v-var->e-var (->> (for [[e clauses] e-var->v-var-clauses
                                   {:keys [e v]} clauses
                                   :when (not (contains? e-vars v))]
                               [v e])
                             (into {}))
           var->joins {}
           e-var->literal-v-clauses (->> (for [{:keys [e v]
                                                :as clause} bgp-clauses
                                               :when (and (logic-var? e)
                                                          (not (logic-var? v)))]
                                           clause)
                                         (group-by :e))
           [var->joins var->names] (e-var-literal-v-joins snapshot e-var->literal-v-clauses var->joins
                                                          var->names business-time transact-time)
           [var->joins var->names] (e-var-literal-v-or-joins snapshot or-clauses var->joins
                                                             var->names business-time transact-time)
           v-var->literal-e-clauses (->> (for [clause bgp-clauses
                                               :when (and (not (logic-var? (:e clause)))
                                                          (logic-var? (:v clause)))]
                                           clause)
                                         (group-by :v))
           leaf-v-var? (fn [e v]
                         (and (= (count (get var->names v)) 1)
                              (or (contains? e-var->literal-v-clauses e)
                                  (contains? v-var->literal-e-clauses v))
                              (->> (for [vars [unification-vars not-vars or-vars e-vars]]
                                     (not (contains? vars v)))
                                   (every? true?))))
           e-var+v-var->join-clauses (->> (for [{:keys [e v] :as clause} bgp-clauses
                                                :when (and (logic-var? v)
                                                           (not (leaf-v-var? e v)))]
                                            clause)
                                          (group-by (juxt :e :v)))
           e-var->leaf-v-var-clauses (->> (for [{:keys [e a v] :as clause} bgp-clauses
                                                :when (and (logic-var? v)
                                                           (leaf-v-var? e v))]
                                            clause)
                                          (group-by :e))
           [var->joins var->names] (e-var-v-var-joins snapshot e-var+v-var->join-clauses v-var->range-constrants
                                                      var->joins var->names business-time transact-time)
           [var->joins var->names] (v-var-literal-e-joins snapshot object-store v-var->literal-e-clauses v-var->range-constrants
                                                          var->joins var->names business-time transact-time)
           v-var-name->attr (->> (for [{:keys [a v]} bgp-clauses
                                       :when (logic-var? v)
                                       var-name (get var->names v)]
                                   [var-name a])
                                 (into {}))
           e-var-name->attr (zipmap (mapcat var->names e-vars)
                                    (repeat :crux.db/id))
           var-names->attr (merge v-var-name->attr e-var-name->attr)
           predicate-vars (for [{:keys [pred-fn args]
                                 :as clause} pred-clauses
                                arg args
                                :when (logic-var? arg)]
                            (if (contains? var->names arg)
                              arg
                              (throw (IllegalArgumentException.
                                      (str "Predicate refers to unknown variable: "
                                           arg " " (pr-str clause))))))
           find-and-predicate-vars (distinct (concat find predicate-vars))
           preds (build-preds pred-clauses)
           var+joins (vec var->joins)
           var->v-result-index (zipmap (map key var+joins) (range))
           unification-preds (build-unification-preds unify-clauses var->names var->v-result-index)
           not-constraints (build-not-constraints object-store not-clauses e-var->leaf-v-var-clauses
                                                  var->names var-names->attr v-var->e-var)
           shared-names (mapv var->names e-vars)
           constrain-query-result-fn (fn [max-ks result]
                                       (some->> (doc/constrain-join-result-by-names shared-names max-ks result)
                                                (constrain-join-result-by-unification unification-preds max-ks)
                                                (constrain-join-result-by-not not-constraints var->joins max-ks)
                                                (constrain-join-result-by-join-keys var->v-result-index var->names shared-e-v-vars max-ks)))]
       (doseq [var find
               :when (not (contains? var->names var))]
         (throw (IllegalArgumentException. (str "Find clause references unbound variable: " var))))
       (for [[_ join-results] (doc/idx->seq (doc/new-n-ary-join-virtual-index (mapv val var+joins)
                                                                              constrain-query-result-fn))
             result (cartesian-product
                     (for [var find-and-predicate-vars]
                       (results-for-var object-store e-var->leaf-v-var-clauses var->names var-names->attr v-var->e-var join-results var)))
             :let [values (map :value result)]
             :when (or (nil? preds) (preds (zipmap find-and-predicate-vars values)))]
         (with-meta
           (vec (take (count find) values))
           (let [find-results (vec (take (count find) result))]
             (zipmap (map :var find-results) find-results))))))))
