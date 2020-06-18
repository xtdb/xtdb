(ns ^:no-doc crux.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [com.stuartsierra.dependency :as dep]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.bus :as bus]
            [crux.api :as api]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Box ExceptionInfo]
           (crux.api ICruxDatasource HistoryOptions HistoryOptions$SortOrder NodeOutOfSyncException)
           crux.codec.EntityTx
           crux.index.IndexStoreIndexState
           (java.io Closeable)
           (java.util Comparator Date List UUID)
           [java.util.concurrent Executors ExecutorService TimeoutException TimeUnit]))

(defn- logic-var? [x]
  (symbol? x))

(def ^:private literal? (complement (some-fn vector? logic-var?)))

(declare pred-constraint)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next)
         spec))

(def ^:private built-ins '#{and})

(defn- pred-constraint? [x]
  (contains? (methods pred-constraint) x))

(s/def ::triple (s/and vector? (s/cat :e (some-fn logic-var? c/valid-id? set?)
                                      :a (s/and c/valid-id? some?)
                                      :v (s/? (some-fn logic-var? literal?)))))

(s/def ::pred-fn (s/and symbol?
                        (complement built-ins)
                        (s/conformer #(or (if (pred-constraint? %)
                                            %
                                            (some->> (if (qualified-symbol? %)
                                                       (requiring-resolve %)
                                                       (ns-resolve 'clojure.core %))
                                                     (var-get)))
                                          %))
                        (some-fn fn? logic-var?)))
(s/def ::pred (s/and vector? (s/cat :pred (s/and seq?
                                                 (s/cat :pred-fn ::pred-fn
                                                        :args (s/* any?)))
                                    :return (s/? logic-var?))))

(s/def ::rule (s/and list? (s/cat :name (s/and symbol? (complement built-ins))
                                  :args (s/+ any?))))

(s/def ::range-op '#{< <= >= > =})
(s/def ::range (s/tuple (s/and list?
                               (s/or :sym-val (s/cat :op ::range-op
                                                     :sym logic-var?
                                                     :val literal?)
                                     :val-sym (s/cat :op ::range-op
                                                     :val literal?
                                                     :sym logic-var?)
                                     :sym-sym (s/cat :op ::range-op
                                                     :sym-a logic-var?
                                                     :sym-b logic-var?)))))

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

(s/def ::timeout nat-int?)

(declare normalize-query open-index-store build-sub-query)

(s/def ::query (s/and (s/conformer #'normalize-query)
                      (s/keys :req-un [::find ::where] :opt-un [::args ::rules ::offset ::limit ::order-by ::timeout ::full-results?])))

(defmulti pred-args-spec first)

(defmethod pred-args-spec 'get-attr [_]
  (s/cat :pred-fn  #{'get-attr} :args (s/spec (s/cat :e-var logic-var? :attr literal? :not-found (s/? literal?))) :return logic-var?))

(defmethod pred-args-spec '== [_]
  (s/cat :pred-fn #{'==} :args (s/tuple some? some?)))

(defmethod pred-args-spec '!= [_]
  (s/cat :pred-fn #{'!=} :args (s/tuple some? some?)))

(defmethod pred-args-spec :default [_]
  (s/cat :pred-fn (s/or :var logic-var? :fn fn?) :args (s/coll-of any?) :return (s/? logic-var?)))

(s/def ::pred-args (s/multi-spec pred-args-spec first))

;; NOTE: :min-count generates boxed math warnings, so this goes below
;; the spec.
(set! *unchecked-math* :warn-on-boxed)

(defmulti pred-constraint
  (fn [{:keys [pred return] {:keys [pred-fn]} :pred :as clause} encode-value-fn idx-id arg-bindings]
    pred-fn))

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
                                          >= (comp not neg? compare)
                                          = =})

(def ^:private range->inverse-range '{< >
                                      <= >=
                                      > <
                                      >= <=
                                      = =})

(defn- rewrite-self-join-triple-clause [{:keys [e v] :as triple}]
  (let [v-var (gensym (str "self-join_" v "_"))]
    {:triple [(with-meta
                (assoc triple :v v-var)
                {:self-join? true})]
     :pred [{:pred {:pred-fn '== :args [v-var e]}}]}))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         (if (= :triple type)
           (let [{:keys [e v] :as clause} (normalize-triple-clause clause)]
             (if (and (logic-var? e) (= e v))
               (rewrite-self-join-triple-clause clause)
               {:triple [clause]}))

           (case type
             :pred {:pred [(let [{:keys [pred]} clause
                                 {:keys [pred-fn args]} pred]
                             (if-let [range-pred (and (= 2 (count args))
                                                      (every? logic-var? args)
                                                      (get pred->built-in-range-pred pred-fn))]
                               (assoc-in clause [:pred :pred-fn] range-pred)
                               clause))]}

             :range (let [[order clause] (first clause)
                          [order clause] (if (= :sym-sym order) ;; NOTE: to deal with rule expansion
                                           (let [{:keys [op sym-a sym-b]} clause]
                                             (cond
                                               (literal? sym-a)
                                               [:val-sym {:op op :val sym-a :sym sym-b}]
                                               (literal? sym-b)
                                               [:sym-val {:op op :val sym-b :sym sym-a}]
                                               :else
                                               [order clause]))
                                           [order clause])
                          {:keys [op sym val] :as clause} (cond-> clause
                                                            (= :val-sym order) (update :op range->inverse-range))]
                      (if (and (not= :sym-sym order)
                               (not (logic-var? sym)))
                        {:pred [{:pred {:pred-fn (get pred->built-in-range-pred (var-get (resolve op)))
                                        :args [sym val]}}]}
                        {:range [clause]}))

             {type [clause]})))

       (apply merge-with into)))

(defn- collect-vars [{triple-clauses :triple
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
     :not-vars (->> (vals not-vars)
                    (reduce into not-join-vars))
     :pred-vars (set (for [{:keys [pred return]} pred-clauses
                           arg (cons return (cond->> (:args pred)
                                              (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred))))
                           :when (logic-var? arg)]
                       arg))
     :pred-return-vars (set (for [{:keys [pred return]} pred-clauses
                                  :when (logic-var? return)]
                              return))
     :range-vars (set (for [{:keys [sym sym-a sym-b]} range-clauses
                            sym [sym sym-a sym-b]
                            :when (logic-var? sym)]
                        sym))
     :or-vars (apply set/union (vals or-vars))
     :rule-vars (set/union (set (for [{:keys [args]} rule-clauses
                                      arg args
                                      :when (logic-var? arg)]
                                  arg))
                           or-join-vars)}))

(defn- arg-for-var [arg var]
  (second
    (or (find arg (symbol (name var)))
        (find arg (keyword (name var))))))

(defn- new-binary-index [{:keys [e a v] :as clause} {:keys [entity-resolver-fn]} index-store {:keys [vars-in-join-order]}]
  (let [order (keep #{e v} vars-in-join-order)
        nested-index-store (db/open-nested-index-store index-store)]
    (if (= v (first order))
      (let [v-idx (idx/new-index-store-index
                   (fn [k]
                     (db/av nested-index-store a k entity-resolver-fn)))
            e-idx (idx/new-index-store-index
                   (fn [k]
                     (db/ave nested-index-store a (.key ^IndexStoreIndexState (.state v-idx)) k entity-resolver-fn)))]
        (log/debug :join-order :ave (cio/pr-edn-str v) e (cio/pr-edn-str clause))
        (idx/new-n-ary-join-layered-virtual-index [v-idx e-idx]))
      (let [e-idx (idx/new-index-store-index
                   (fn [k]
                     (db/ae nested-index-store a k entity-resolver-fn)))
            v-idx (idx/new-index-store-index
                   (fn [k]
                     (db/aev nested-index-store a (.key ^IndexStoreIndexState (.state e-idx)) k entity-resolver-fn)))]
        (log/debug :join-order :aev e (cio/pr-edn-str v) (cio/pr-edn-str clause))
        (idx/new-n-ary-join-layered-virtual-index [e-idx v-idx])))))

(defn- sort-triple-clauses [stats triple-clauses]
  (sort-by (fn [{:keys [a]}]
             (get stats a 0)) triple-clauses))

(defn- new-literal-index [index-store v]
  (let [encode-value-fn (partial db/encode-value index-store)]
    (if (c/multiple-values? v)
      (idx/new-relation-virtual-index (mapv vector v) 1 encode-value-fn)
      (idx/new-singleton-virtual-index v encode-value-fn))))

(defn- triple-joins [triple-clauses  var->joins arg-vars range-vars stats]
  (let [var->frequency (->> (concat (map :e triple-clauses)
                                    (map :v triple-clauses)
                                    range-vars)
                            (filter logic-var?)
                            (frequencies))
        triple-clauses (sort-triple-clauses stats triple-clauses)
        literal-clauses (for [{:keys [e v] :as clause} triple-clauses
                              :when (or (literal? e)
                                        (literal? v))]
                          clause)
        literal-join-order (concat (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       v
                                       e))
                                   arg-vars
                                   (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       e
                                       v)))
        self-join-clauses (filter (comp :self-join? meta) triple-clauses)
        self-join-vars (map :v self-join-clauses)
        join-order (loop [join-order (concat literal-join-order self-join-vars)
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
             (let [join {:id (gensym "triple")
                         :idx-fn (fn [db index-store compiled-query]
                                   (new-binary-index clause
                                                     db
                                                     index-store
                                                     compiled-query))}
                   var->joins (merge-with into var->joins {v [join]
                                                           e [join]})
                   var->joins (if (literal? e)
                                (merge-with into var->joins {e [{:idx-fn
                                                                 (fn [db index-store compiled-query]
                                                                   (new-literal-index index-store e))}]})
                                var->joins)
                   var->joins (if (literal? v)
                                (merge-with into var->joins {v [{:idx-fn
                                                                 (fn [db index-store compiled-query]
                                                                   (new-literal-index index-store v))}]})
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
                :idx-fn
                (fn [_ index-store _]
                  (idx/new-relation-virtual-index []
                                                  (count arg-vars)
                                                  (partial db/encode-value index-store)))}]
      [idx-id
       (->> arg-vars
            (reduce
             (fn [var->joins arg-var]
               (->> {arg-var [join]}
                    (merge-with into var->joins)))
             var->joins))])
    [nil var->joins]))

(defn- pred-joins [pred-clauses var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause+idx-ids var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [idx-id (gensym "pred-return")
                  join {:id idx-id
                        :idx-fn
                        (fn [_ index-store _]
                          (idx/new-relation-virtual-index []
                                                          1
                                                          (partial db/encode-value index-store)))}]
              [(conj pred-clause+idx-ids [pred-clause idx-id])
               (merge-with into var->joins {return [join]})])
            [(conj pred-clause+idx-ids [pred-clause])
             var->joins]))
        [[] var->joins])))

;; TODO: This is a naive, but not totally irrelevant measure. Aims to
;; bind variables as early and cheaply as possible.
(defn- clause-complexity [clause]
  (count (cio/pr-edn-str clause)))

(defn- single-e-var-triple? [vars where]
  (and (= 1 (count where))
       (let [[[type {:keys [e v]}]] where]
         (and (= :triple type)
              (contains? vars e)
              (logic-var? e)
              (literal? v)
              (not (c/multiple-values? v))))))

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
                                              (str "Or join variable never used: " var " " (cio/pr-edn-str clause))))))
                                  {:or-vars or-vars
                                   :free-vars free-vars
                                   :bound-vars bound-vars
                                   :where where
                                   :single-e-var-triple? (single-e-var-triple? bound-vars where)}))
                free-vars (:free-vars (first or-branches))
                idx-id (gensym "or-free-vars")
                join (when (seq free-vars)
                       {:id idx-id
                        :idx-fn
                        (fn [_ index-store _]
                          (idx/new-relation-virtual-index []
                                                          (count free-vars)
                                                          (partial db/encode-value index-store)))})]
            (when (not (apply = (map :or-vars or-branches)))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variables: " (cio/pr-edn-str clause)))))
            [(conj or-clause+idx-id+or-branches [clause idx-id or-branches])
             (into known-vars free-vars)
             (apply merge-with into var->joins (for [v free-vars]
                                                 {v [join]}))]))
        [[] known-vars var->joins])))

(defrecord VarBinding [e-var var attr result-index result-name type value?])

(defn- build-var-bindings [var->attr v-var->e e->v-var var->values-result-index max-join-depth vars]
  (->> (for [var vars
             :let [e-var (get v-var->e var var)]]
         [var (map->VarBinding
               {:e-var e-var
                :var var
                :attr (get var->attr var)
                :result-index (get var->values-result-index var)
                :result-name e-var
                :type :entity
                :value? false})])
       (into {})))

(defn- value-var-binding [var result-index type]
  (map->VarBinding
   {:var var
    :result-name (symbol "crux.query.value" (name var))
    :result-index result-index
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

(defn- calculate-constraint-join-depth [var->bindings vars]
  (->> (for [var vars]
         (get-in var->bindings [var :result-index] -1))
       (apply max -1)
       (long)
       (inc)))

(defn- new-range-constraint-wrapper-fn [op ^Box val]
  (case op
    = #(idx/new-equals-virtual-index % val)
    < #(-> (idx/new-less-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    <= #(-> (idx/new-less-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    > #(-> (idx/new-greater-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    >= #(-> (idx/new-greater-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))))

;; TODO: Get rid of assumption that value-buffer-type-id is always one
;; byte. Or better, move construction or handling of ranges to the
;; IndexStore and remove the need for the type-prefix completely.
(defn- build-var-range-constraints [encode-value-fn range-clauses]
  (->> (for [[var clauses] (group-by :sym range-clauses)]
         [var (->> (for [{:keys [op val sym]} clauses
                         :when (logic-var? sym)
                         :let [val (encode-value-fn val)]]
                     (new-range-constraint-wrapper-fn op (Box. val)))
                   (apply comp))])
       (into {})))

(defn- build-logic-var-range-constraint-fns [encode-value-fn range-clauses var->bindings]
  (->> (for [{:keys [op sym-a sym-b] :as clause} range-clauses
             :when (and (logic-var? sym-a)
                        (logic-var? sym-b))
             :let [sym+index [[sym-a (.result-index ^VarBinding (get var->bindings sym-a))]
                              [sym-b (.result-index ^VarBinding (get var->bindings sym-b))]]
                   [[first-sym first-index]
                    [second-sym second-index] :as sym+index] (sort-by second sym+index)
                   op (if (= sym-a first-sym)
                        (get range->inverse-range op)
                        op)]]
         {second-sym
          [(fn []
             (let [range-join-depth (calculate-constraint-join-depth var->bindings [first-sym])
                   val (Box. mem/empty-buffer)]
               {:join-depth range-join-depth
                :range-constraint-wrapper-fn (new-range-constraint-wrapper-fn op val)
                :constraint-fn (fn range-constraint [index-store db idx-id->idx ^List join-keys]
                                 (set! (.-val val) (.get join-keys first-index))
                                 true)}))]})
       (apply merge-with into {})))

(defn- bound-result-for-var [index-store ^VarBinding var-binding ^List join-keys]
  (db/decode-value index-store (.get join-keys (.result-index var-binding))))

(defn- validate-existing-vars [var->bindings clause vars]
  (doseq [var vars
          :when (not (or (pred-constraint? var)
                         (contains? var->bindings var)))]
    (throw (IllegalArgumentException.
            (str "Clause refers to unknown variable: "
                 var " " (cio/pr-edn-str clause))))))

(defmethod pred-constraint 'get-attr [{:keys [pred return] {:keys [pred-fn]} :pred :as clause} encode-value-fn idx-id arg-bindings]
  (let [arg-bindings (rest arg-bindings)
        [e-var attr not-found] arg-bindings
        not-found? (= 3 (count arg-bindings))
        not-found (mapv encode-value-fn (c/vectorize-value not-found))
        e-result-index (.result-index ^VarBinding e-var)]
    (fn pred-get-attr-constraint [index-store {:keys [entity-resolver-fn] :as db} idx-id->idx ^List join-keys]
      (let [e (.get join-keys e-result-index)
            vs (db/aev index-store attr e nil entity-resolver-fn)]
        (if-let [values (if (and (empty? vs) not-found?)
                          not-found
                          (not-empty vs))]
          (do (idx/update-relation-virtual-index! (get idx-id->idx idx-id) values identity true)
              true)
          false)))))

(defn- built-in-unification-pred [unifier-fn encode-value-fn arg-bindings]
  (let [arg-bindings (vec (for [arg-binding (rest arg-bindings)]
                            (if (instance? VarBinding arg-binding)
                              arg-binding
                              (->> (map encode-value-fn (c/vectorize-value arg-binding))
                                   (into (sorted-set-by mem/buffer-comparator))))))]
    (fn unification-constraint [index-store db idx-id->idx ^List join-keys]
      (let [values (for [arg-binding arg-bindings]
                     (if (instance? VarBinding arg-binding)
                       (sorted-set-by mem/buffer-comparator (.get join-keys (.result-index ^VarBinding arg-binding)))
                       arg-binding))]
        (unifier-fn values)))))

(defmethod pred-constraint '== [{:keys [pred return] {:keys [pred-fn]} :pred :as clause} encode-value-fn idx-id arg-bindings]
  (built-in-unification-pred #(boolean (not-empty (apply set/intersection %))) encode-value-fn arg-bindings))

(defmethod pred-constraint '!= [{:keys [pred return] {:keys [pred-fn]} :pred :as clause} encode-value-fn idx-id arg-bindings]
  (built-in-unification-pred #(empty? (apply set/intersection %)) encode-value-fn arg-bindings))

(defmethod pred-constraint :default [{:keys [pred return] {:keys [pred-fn]} :pred :as clause} encode-value-fn idx-id arg-bindings]
  (fn pred-constraint [index-store db idx-id->idx join-keys]
    (let [[pred-fn & args] (for [arg-binding arg-bindings]
                             (if (instance? VarBinding arg-binding)
                               (bound-result-for-var index-store arg-binding join-keys)
                               arg-binding))]
      (let [pred-result (apply pred-fn args)]
        (if return
          (if-let [values (not-empty (mapv vector (c/vectorize-value pred-result)))]
            (do (idx/update-relation-virtual-index! (get idx-id->idx idx-id) values)
                true)
            false)
          pred-result)))))

(defn- build-pred-constraints [encode-value-fn pred-clause+idx-ids var->bindings]
  (for [[{:keys [pred return] :as clause} idx-id] pred-clause+idx-ids
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)
              arg-bindings (for [arg (cons pred-fn args)]
                             (if (and (logic-var? arg)
                                      (not (pred-constraint? arg)))
                               (get var->bindings arg)
                               arg))]]
    (do (validate-existing-vars var->bindings clause pred-vars)
        (s/assert ::pred-args (cond-> [pred-fn (vec args)]
                                return (conj return)))
        {:join-depth pred-join-depth
         :constraint-fn (pred-constraint clause encode-value-fn idx-id arg-bindings)})))

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
(defn- or-single-e-var-triple-fast-path [index-store {:keys [entity-resolver-fn] :as db} {:keys [e a v] :as clause} args]
  (let [eid (get (first args) e)
        v (db/encode-value index-store v)
        [found-v] (db/aev index-store a eid v entity-resolver-fn)]
    (when (and found-v (mem/buffers=? v found-v))
      [])))

(def ^:private ^:dynamic *recursion-table* {})

;; TODO: This tabling mechanism attempts at avoiding infinite
;; recursion, but does not actually cache anything. Short-circuits
;; identical sub trees. Passes tests, unsure if this really works in
;; the general case. Depends on the eager expansion of rules for some
;; cases to pass. One alternative is maybe to try to cache the
;; sequence and reuse it, somehow detecting if it loops.
(defn- build-or-constraints
  [rule-name->rules or-clause+idx-id+or-branches var->bindings vars-in-join-order stats]
  (for [[clause idx-id [{:keys [free-vars bound-vars]} :as or-branches]] or-clause+idx-id+or-branches
        :let [or-join-depth (calculate-constraint-join-depth var->bindings bound-vars)
              free-vars-in-join-order (filter (set free-vars) vars-in-join-order)
              has-free-vars? (boolean (seq free-vars))
              bound-var-bindings (map var->bindings bound-vars)
              {:keys [rule-name]} (meta clause)]]
    (do (validate-existing-vars var->bindings clause bound-vars)
        {:join-depth or-join-depth
         :constraint-fn
         (fn or-constraint [index-store db idx-id->idx join-keys]
           (let [args (when (seq bound-vars)
                        [(->> (for [var-binding bound-var-bindings]
                                (bound-result-for-var index-store var-binding join-keys))
                              (zipmap bound-vars))])
                 branch-results (for [[branch-index {:keys [where
                                                            single-e-var-triple?] :as or-branch}] (map-indexed vector or-branches)
                                      :let [cache-key (when rule-name
                                                        [rule-name branch-index (count free-vars) (set (mapv vals args))])
                                            cached-result (when cache-key
                                                            (get *recursion-table* cache-key))]]
                                  (with-open [index-store ^Closeable (open-index-store db)]
                                    (let [db (assoc db :index-store index-store)]
                                      (cond
                                        cached-result
                                        cached-result

                                        single-e-var-triple?
                                        (let [[[_ clause]] where]
                                          (or-single-e-var-triple-fast-path
                                           index-store
                                           db
                                           clause
                                           args))

                                        :else
                                        (binding [*recursion-table* (if cache-key
                                                                      (assoc *recursion-table* cache-key [])
                                                                      *recursion-table*)]
                                          (let [{:keys [n-ary-join
                                                        var->bindings]} (build-sub-query index-store db where args rule-name->rules stats)
                                                free-vars-in-join-order-bindings (map var->bindings free-vars-in-join-order)]
                                            (when-let [idx-seq (seq (idx/layered-idx->seq n-ary-join))]
                                              (if has-free-vars?
                                                (vec (for [join-keys idx-seq]
                                                       (vec (for [var-binding free-vars-in-join-order-bindings]
                                                              (bound-result-for-var index-store var-binding join-keys)))))
                                                []))))))))]
             (when (seq (remove nil? branch-results))
               (when has-free-vars?
                 (let [free-results (->> branch-results
                                         (apply concat)
                                         (distinct)
                                         (vec))]
                   (idx/update-relation-virtual-index! (get idx-id->idx idx-id) free-results)))
               true)))})))

(defn- build-not-constraints [rule-name->rules not-type not-clauses var->bindings stats]
  (for [not-clause not-clauses
        :let [[not-vars not-clause] (case not-type
                                      :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                            not-clause]
                                      :not-join [(:args not-clause)
                                                 (:body not-clause)])
              not-vars (remove blank-var? not-vars)
              not-var-bindings (map var->bindings not-vars)
              not-join-depth (calculate-constraint-join-depth var->bindings not-vars)]]
    (do (validate-existing-vars var->bindings not-clause not-vars)
        {:join-depth not-join-depth
         :constraint-fn
         (fn not-constraint [index-store db idx-id->idx join-keys]
           (with-open [index-store ^Closeable (open-index-store db)]
             (let [db (assoc db :index-store index-store)
                   args (when (seq not-vars)
                          [(->> (for [var-binding not-var-bindings]
                                  (bound-result-for-var index-store var-binding join-keys))
                                (zipmap not-vars))])
                   {:keys [n-ary-join]} (build-sub-query index-store db not-clause args rule-name->rules stats)]
               (empty? (idx/layered-idx->seq n-ary-join)))))})))

(defn- constrain-join-result-by-constraints [index-store db idx-id->idx depth->constraints join-keys]
  (->> (get depth->constraints (count join-keys))
       (every? (fn [f]
                 (f index-store db idx-id->idx join-keys)))))

(defn- potential-bgp-pair-vars [g vars]
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
                   pred-vars (into pred-vars (potential-bgp-pair-vars triple-join-deps pred-vars))]
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
             (let [bound-vars (into bound-vars (potential-bgp-pair-vars triple-join-deps bound-vars))]
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
                       (str "Unknown rule: " (cio/pr-edn-str sub-clause)))))
             (let [rule-args+body (for [{:keys [head body]} rules]
                                    [(vec (concat (:bound-args head)
                                                  (:args head)))
                                     body])
                   [arity :as arities] (->> rule-args+body
                                            (map (comp count first))
                                            (distinct))]
               (when-not (= 1 (count arities))
                 (throw (IllegalArgumentException. (str "Rule definitions require same arity: " (cio/pr-edn-str rules)))))
               (when-not (= arity (count (:args clause)))
                 (throw (IllegalArgumentException.
                         (str "Rule invocation has wrong arity, expected: " arity " " (cio/pr-edn-str sub-clause)))))
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
                               (if (->> (cond->> (:args pred)
                                          (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred)))
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

(defn- expand-leaf-preds [{triple-clauses :triple
                           pred-clauses :pred
                           :as type->clauses}
                          arg-vars
                          stats]
  (let [collected-vars (collect-vars type->clauses)
        invalid-leaf-vars (set (concat arg-vars (:e-vars collected-vars)))
        non-leaf-v-vars (set (for [[v-var non-leaf-group] (group-by :v triple-clauses)
                                   :when (> (count non-leaf-group) 1)]
                               v-var))
        potential-leaf-v-vars (set/difference (:v-vars collected-vars) invalid-leaf-vars non-leaf-v-vars)
        leaf-groups (->> (for [[e-var leaf-group] (group-by :e (filter (comp potential-leaf-v-vars :v) triple-clauses))
                               :when (> (count leaf-group) 1)]
                           [e-var (sort-triple-clauses stats leaf-group)])
                         (into {}))
        leaf-triple-clauses (set (mapcat next (vals leaf-groups)))
        triple-clauses (remove leaf-triple-clauses triple-clauses)
        leaf-preds (for [{:keys [e a v]} leaf-triple-clauses]
                     {:pred {:pred-fn 'get-attr :args [e a]}
                      :return v})]
    (assoc type->clauses
           :triple triple-clauses
           :pred (vec (concat pred-clauses leaf-preds)))))

(defn- update-depth->constraints [depth->join-depth constraints]
  (reduce
   (fn [acc {:keys [join-depth constraint-fn]}]
     (update acc join-depth (fnil conj []) constraint-fn))
   depth->join-depth
   constraints))

(defn- compile-sub-query [encode-value-fn where arg-vars rule-name->rules stats]
  (let [where (expand-rules where rule-name->rules {})
        {triple-clauses :triple
         range-clauses :range
         pred-clauses :pred
         not-clauses :not
         not-join-clauses :not-join
         or-clauses :or
         or-join-clauses :or-join
         :as type->clauses} (expand-leaf-preds (normalize-clauses where) arg-vars stats)
        {:keys [e-vars
                v-vars
                range-vars
                pred-vars
                pred-return-vars]} (collect-vars type->clauses)
        var->joins {}
        [triple-join-deps var->joins] (triple-joins triple-clauses
                                                    var->joins
                                                    arg-vars
                                                    range-vars
                                                    stats)
        [args-idx-id var->joins] (arg-joins arg-vars
                                            e-vars
                                            var->joins)
        [pred-clause+idx-ids var->joins] (pred-joins pred-clauses var->joins)
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
        var->range-constraints (build-var-range-constraints encode-value-fn range-clauses)
        var->logic-var-range-constraint-fns (build-logic-var-range-constraint-fns encode-value-fn range-clauses var->bindings)
        not-constraints (build-not-constraints rule-name->rules :not not-clauses var->bindings stats)
        not-join-constraints (build-not-constraints rule-name->rules :not-join not-join-clauses var->bindings stats)
        pred-constraints (build-pred-constraints encode-value-fn pred-clause+idx-ids var->bindings)
        or-constraints (build-or-constraints rule-name->rules or-clause+idx-id+or-branches
                                             var->bindings vars-in-join-order stats)
        depth->constraints (->> (concat pred-constraints
                                        not-constraints
                                        not-join-constraints
                                        or-constraints)
                                (update-depth->constraints (vec (repeat join-depth nil))))]
    {:depth->constraints depth->constraints
     :var->range-constraints var->range-constraints
     :var->logic-var-range-constraint-fns var->logic-var-range-constraint-fns
     :vars-in-join-order vars-in-join-order
     :var->joins var->joins
     :var->bindings var->bindings
     :arg-vars-in-join-order arg-vars-in-join-order
     :args-idx-id args-idx-id
     :attr-stats (select-keys stats (vals var->attr))}))

(defn- build-idx-id->idx [db index-store {:keys [var->joins] :as compiled-query}]
  (->> (for [[_ joins] var->joins
             {:keys [id idx-fn] :as join} joins
             :when id]
         [id idx-fn])
       (reduce
        (fn [acc [id idx-fn]]
          (if (contains? acc id)
            acc
            (assoc acc id (idx-fn db index-store compiled-query))))
        {})))

(defn- add-logic-var-constraints [{:keys [var->logic-var-range-constraint-fns]
                                   :as compiled-query}]
  (if (seq var->logic-var-range-constraint-fns)
    (let [logic-var+range-constraint (for [[v fs] var->logic-var-range-constraint-fns
                                           f fs]
                                       [v (f)])]
      (-> compiled-query
          (update :depth->constraints update-depth->constraints (map second logic-var+range-constraint))
          (update :var->range-constraints (fn [var->range-constraints]
                                            (reduce (fn [acc [v {:keys [range-constraint-wrapper-fn]}]]
                                                      (update acc v (fn [x]
                                                                      (cond-> range-constraint-wrapper-fn
                                                                        x (comp x)))))
                                                    var->range-constraints
                                                    logic-var+range-constraint)))))
    compiled-query))

(defn- build-sub-query [index-store {:keys [query-engine] :as db} where args rule-name->rules stats]
  ;; NOTE: this implies argument sets with different vars get compiled
  ;; differently.
  (let [arg-vars (arg-vars args)
        encode-value-fn (partial db/encode-value index-store)
        {:keys [query-cache]} query-engine
        {:keys [depth->constraints
                vars-in-join-order
                var->range-constraints
                var->logic-var-range-constraint-fns
                var->joins
                var->bindings
                arg-vars-in-join-order
                args-idx-id
                attr-stats]
         :as compiled-query} (-> (lru/compute-if-absent
                                  query-cache
                                  [where arg-vars rule-name->rules]
                                  identity
                                  (fn [_]
                                    (compile-sub-query encode-value-fn where arg-vars rule-name->rules stats)))
                                 (add-logic-var-constraints))
        idx-id->idx (build-idx-id->idx db index-store compiled-query)
        unary-join-indexes (for [v vars-in-join-order]
                             (-> (idx/new-unary-join-virtual-index
                                  (vec (for [{:keys [id idx-fn] :as join} (get var->joins v)]
                                         (or (get idx-id->idx id)
                                             (idx-fn db index-store compiled-query)))))
                                 (idx/wrap-with-range-constraints (get var->range-constraints v))))
        constrain-result-fn (fn [join-keys]
                              (constrain-join-result-by-constraints index-store db idx-id->idx depth->constraints join-keys))]
    (when (and (seq args) args-idx-id)
      (binding [nippy/*freeze-fallback* :write-unfreezable]
        (idx/update-relation-virtual-index!
         (get idx-id->idx args-idx-id)
         (vec (for [arg (distinct args)]
                (mapv #(arg-for-var arg %) arg-vars-in-join-order))))))
    (log/debug :where (cio/pr-edn-str where))
    (log/debug :vars-in-join-order vars-in-join-order)
    (log/debug :attr-stats (cio/pr-edn-str attr-stats))
    (log/debug :var->bindings (cio/pr-edn-str var->bindings))
    {:n-ary-join (when (constrain-result-fn [])
                   (-> (idx/new-n-ary-join-layered-virtual-index unary-join-indexes)
                       (idx/new-n-ary-constraining-layered-virtual-index constrain-result-fn)))
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

(defn query-plan-for [q encode-value-fn stats]
  (s/assert ::query q)
  (let [{:keys [where args rules]} (s/conform ::query q)]
    (compile-sub-query encode-value-fn where (arg-vars args) (rule-name->rules rules) stats)))

(defn- build-full-results [{:keys [entity-resolver-fn index-store], {:keys [document-store]} :query-engine, :as db} bound-result-tuple]
  (vec (for [value bound-result-tuple]
         (if-let [content-hash (some-> (entity-resolver-fn (c/->id-buffer value)) (c/new-id))]
           (-> (db/fetch-docs document-store #{content-hash})
               (get content-hash))
           value))))

(defn open-index-store ^java.io.Closeable [{:keys [query-engine index-store] :as db}]
  (if index-store
    (db/open-nested-index-store index-store)
    (db/open-index-store (:indexer query-engine))))

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
                           (when-not (s/valid? ::query q)
                             (throw (ex-info (str "Spec assertion failed\n" (s/explain-str ::query q)) (s/explain-data ::query q))))
                           (let [q (normalize-query q)]
                             (->ConformedQuery q (s/conform ::query q)))))]
    (if args
      (do
        (when-not (s/valid? ::args args)
          (throw (ex-info (str "Spec assertion failed\n" (s/explain-str ::args args)) (s/explain-data ::args args))))
        (-> conformed-query
            (assoc-in [:q-normalized :args] args)
            (assoc-in [:q-conformed :args] args)))
      conformed-query)))

(defn- with-entity-resolver-cache [entity-resolver-fn options]
  (let [entity-cache (lru/new-cache (::entity-cache-size options))]
    (fn [k]
      (lru/compute-if-absent entity-cache k mem/copy-to-unpooled-buffer entity-resolver-fn))))

(defn new-entity-resolver-fn [{:keys [valid-time transact-time query-engine index-store] :as db}]
  (let [{:keys [options]} query-engine]
    (cond-> #(db/entity-as-of-resolver index-store % valid-time transact-time)
      (::entity-cache? options) (with-entity-resolver-cache options))))

(defn query
  ([{:keys [valid-time transact-time query-engine] :as db} ^ConformedQuery conformed-q]
   (let [{:keys [^ExecutorService query-executor indexer options]} query-engine
         start-time (System/currentTimeMillis)
         q (.q-normalized conformed-q)
         query-future (.submit query-executor
                               ^Callable
                               (fn []
                                 (try
                                   (with-open [index-store (open-index-store db)]
                                     (let [result-coll-fn (if (:order-by q) vec set)
                                           result (result-coll-fn (query db index-store conformed-q))]
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
                                     t))))
         result (or (try
                      (deref query-future (or (:timeout q) (::query-timeout options)) nil)
                      (catch InterruptedException e
                        (future-cancel query-future)
                        (throw e)))
                    (do (when-not (future-cancel query-future)
                          (throw (IllegalStateException. "Could not cancel query.")))
                        (throw (TimeoutException. "Query timed out."))))]
     (if (instance? Throwable result)
       (throw result)
       result)))

  ([{:keys [valid-time transact-time query-engine] :as db} index-store ^ConformedQuery conformed-q]
   (let [{:keys [indexer options]} query-engine
         db (assoc db :index-store index-store)
         q (.q-normalized conformed-q)
         q-conformed (.q-conformed conformed-q)
         {:keys [find where args rules offset limit order-by full-results?]} q-conformed
         stats (db/read-index-meta indexer :crux.kv/stats)]
     (log/debug :query (cio/pr-edn-str (-> q
                                           (assoc :arg-keys (mapv (comp set keys) (:args q)))
                                           (dissoc :args))))
     (validate-args args)
     (let [rule-name->rules (rule-name->rules rules)
           db (assoc db :entity-resolver-fn (or (:entity-resolver-fn db)
                                                (new-entity-resolver-fn db)))
           {:keys [n-ary-join
                   var->bindings]} (build-sub-query index-store db where args rule-name->rules stats)
           find-var-bindings (map var->bindings find)]
       (doseq [var find
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Find refers to unknown variable: " var))))
       (doseq [{:keys [var]} order-by
               :when (not (contains? var->bindings var))]
         (throw (IllegalArgumentException.
                 (str "Order by refers to unknown variable: " var))))
       (doseq [{:keys [var]} order-by
               :when (not (some #{var} find))]
         (throw (IllegalArgumentException.
                 (str "Order by requires a var from :find. unreturned var: " var))))

       (cond->> (for [join-keys (idx/layered-idx->seq n-ary-join)
                      :let [bound-result-tuple (for [var-binding find-var-bindings]
                                                 (bound-result-for-var index-store var-binding join-keys))]]
                  (if full-results?
                    (build-full-results db bound-result-tuple)
                    (vec bound-result-tuple)))

         true (dedupe)
         order-by (cio/external-sort (order-by-comparator find order-by))
         offset (drop offset)
         limit (take limit))))))

(defn entity-tx [{:keys [valid-time transact-time] :as db} index-store eid]
  (some-> (db/entity-as-of index-store eid valid-time transact-time)
          (c/entity-tx->edn)))

(defn entity [{{:keys [document-store]} :query-engine, :as db} index-store eid]
  (when-let [content-hash (some-> (entity-tx db index-store eid)
                                  :crux.db/content-hash)]
    (-> (db/fetch-docs document-store #{content-hash})
        (get content-hash)
        (c/keep-non-evicted-doc))))

(defrecord QueryDatasource [query-engine
                            ^Date valid-time
                            ^Date transact-time
                            index-store
                            entity-resolver-fn]
  Closeable
  (close [_]
    (when index-store
      (.close ^Closeable index-store)))

  ICruxDatasource
  (entity [this eid]
    (with-open [index-store (open-index-store this)]
      (entity this index-store eid)))

  (entityTx [this eid]
    (with-open [index-store (open-index-store this)]
      (entity-tx this index-store eid)))

  (query [this query]
    ;; TODO in theory this should 'just' be a call to openQuery that eagerly eval's the results
    (let [{:keys [conform-cache bus]} query-engine
          conformed-query (normalize-and-conform-query conform-cache query)
          query-id (str (UUID/randomUUID))
          safe-query (-> conformed-query .q-normalized (dissoc :args))]
      (when bus
        (bus/send bus {:crux/event-type ::submitted-query
                       ::query safe-query
                       ::query-id query-id}))
      (let [ret (crux.query/query this conformed-query)]
        (when bus
          (bus/send bus {:crux/event-type ::completed-query
                         ::query safe-query
                         ::query-id query-id}))
        ret)))

  (openQuery [this query]
    (let [index-store (open-index-store this)
          {:keys [bus conform-cache]} query-engine
          conformed-query (normalize-and-conform-query conform-cache query)
          query-id (str (UUID/randomUUID))
          safe-query (-> conformed-query .q-normalized (dissoc :args))]
      (when bus
        (bus/send bus {:crux/event-type ::submitted-query
                       ::query safe-query
                       ::query-id query-id}))
      (cio/->cursor (fn []
                      (.close index-store)
                      (when bus
                        (bus/send bus {:crux/event-type ::completed-query
                                       ::query safe-query
                                       ::query-id query-id})))
                    (crux.query/query this index-store conformed-query))))

  (entityHistory [this eid opts]
    (with-open [history (.openEntityHistory this eid opts)]
      (into [] (iterator-seq history))))

  (openEntityHistory [this eid opts]
    (letfn [(inc-date [^Date d] (Date. (inc (.getTime d))))
            (with-upper-bound [^Date d, ^Date upper-bound]
              (if (and d (neg? (compare d upper-bound))) d upper-bound))]
      (let [sort-order (condp = (.sortOrder opts)
                         HistoryOptions$SortOrder/ASC :asc
                         HistoryOptions$SortOrder/DESC :desc)
            with-docs? (.withDocs opts)
            index-store (db/open-index-store (:indexer query-engine))

            opts (cond-> {:with-corrections? (.withCorrections opts)
                          :with-docs? with-docs?
                          :start {:crux.db/valid-time (.startValidTime opts)
                                  :crux.tx/tx-time (.startTransactionTime opts)}
                          :end {:crux.db/valid-time (.endValidTime opts)
                                :crux.tx/tx-time (.endTransactionTime opts)}}
                   (= sort-order :asc)
                   (-> (update-in [:end :crux.db/valid-time]
                                  with-upper-bound (inc-date valid-time))
                       (update-in [:end :crux.tx/tx-time]
                                  with-upper-bound (inc-date transact-time)))

                   (= sort-order :desc)
                   (-> (update-in [:start :crux.db/valid-time]
                                  with-upper-bound valid-time)
                       (update-in [:start :crux.tx/tx-time]
                                  with-upper-bound transact-time)))]

        (cio/->cursor #(.close index-store)
                      (->> (db/entity-history index-store eid sort-order opts)
                           (map (fn [^EntityTx etx]
                                  (cond-> {:crux.tx/tx-time (.tt etx)
                                           :crux.tx/tx-id (.tx-id etx)
                                           :crux.db/valid-time (.vt etx)
                                           :crux.db/content-hash (.content-hash etx)}
                                    with-docs? (assoc :crux.db/doc (-> (db/fetch-docs (:document-store query-engine) #{(.content-hash etx)})
                                                                       (get (.content-hash etx))))))))))))

  (validTime [_] valid-time)
  (transactionTime [_] transact-time))

(defrecord QueryEngine [^ExecutorService query-executor document-store
                        indexer bus
                        query-cache conform-cache
                        options]
  api/DBProvider
  (db [this] (api/db this nil nil))
  (db [this valid-time] (api/db this valid-time nil))
  (db [this valid-time tx-time]
    (let [latest-tx-time (:crux.tx/tx-time (db/latest-completed-tx indexer))
          _ (when (and tx-time (or (nil? latest-tx-time) (pos? (compare tx-time latest-tx-time))))
              (throw (NodeOutOfSyncException. (format "node hasn't indexed the requested transaction: requested: %s, available: %s"
                                                      tx-time latest-tx-time)
                                              tx-time latest-tx-time)))
          valid-time (or valid-time (cio/next-monotonic-date))
          tx-time (or tx-time latest-tx-time valid-time)]

      (->QueryDatasource this
                         valid-time
                         tx-time
                         nil
                         nil)))

  (open-db [this] (api/open-db this nil nil))
  (open-db [this valid-time] (api/open-db this valid-time nil))
  (open-db [this valid-time tx-time]
    (let [db (api/db this valid-time tx-time)
          index-store (open-index-store db)
          db (assoc db :index-store index-store)
          entity-resolver-fn (new-entity-resolver-fn db)]
      (assoc db :entity-resolver-fn entity-resolver-fn)))

  Closeable
  (close [_]
    (when query-executor
      (doto query-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS)))))

(def query-engine
  {:start-fn (fn [{:crux.node/keys [indexer bus document-store]} {::keys [query-pool-size query-cache-size conform-cache-size] :as args}]
               (let [query-executor (Executors/newFixedThreadPool query-pool-size (cio/thread-factory "crux.query.query-pool-thread"))]
                 (map->QueryEngine
                  {:indexer indexer
                   :conform-cache (lru/new-cache conform-cache-size)
                   :query-cache (lru/new-cache query-cache-size)
                   :document-store document-store
                   :bus bus
                   :query-executor query-executor
                   :options args})))
   :deps [:crux.node/indexer :crux.node/bus :crux.node/document-store]
   :args {::query-pool-size {:doc "Query Pool Size"
                             :default 32
                             :crux.config/type :crux.config/nat-int}
          ::query-cache-size {:doc "Compiled Query Cache Size"
                              :default 10240
                              :crux.config/type :crux.config/nat-int}
          ::conform-cache-size {:doc "Conformed Query Cache Size"
                                :default 10240
                                :crux.config/type :crux.config/nat-int}
          ::entity-cache? {:doc "Enable Query Entity Cache"
                           :default true
                           :crux.config/type :crux.config/boolean}
          ::entity-cache-size {:doc "Query Entity Cache Size"
                               :default 10000
                               :crux.config/type :crux.config/nat-int}
          ::query-timeout {:doc "Query Timeout ms"
                           :default 30000
                           :crux.config/type :crux.config/nat-int}}})
