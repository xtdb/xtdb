(ns core2.datalog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.operator :as op]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           java.lang.AutoCloseable
           org.apache.arrow.memory.BufferAllocator))

(s/def ::logic-var
  (s/and simple-symbol?
         (comp #(str/starts-with? % "?") name)))

;; TODO flesh out
(def ^:private eid? (some-fn string? number? inst? keyword?))

(s/def ::eid eid?)
(s/def ::value (some-fn eid?))

(s/def ::aggregate
  (s/cat :aggregate simple-symbol?
         :logic-var ::logic-var))

(s/def ::find-arg
  (s/or :logic-var ::logic-var
        :aggregate ::aggregate))

(defn- find-arg-var [[find-arg-type find-arg-arg]]
  (case find-arg-type
    :logic-var find-arg-arg
    :aggregate (:logic-var find-arg-arg)))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))

(s/def ::args-list (s/coll-of ::logic-var, :kind vector?, :min-count 1))

(s/def ::source
  (s/and simple-symbol?
         (comp #(str/starts-with? % "$") name)))

(s/def ::in-binding
  (s/or :source ::source
        :scalar ::logic-var
        :tuple ::args-list
        :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)))

(defn- in-scalar-vars [[binding-type binding-arg]]
  (case binding-type
    :scalar #{binding-arg}
    :tuple (set binding-arg)
    (:source :collection :relation) #{}))

(s/def ::in (s/* ::in-binding))

;; 'triple' ;)
(s/def ::triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat :src (s/? ::source)
                :e (s/or :literal ::eid, :logic-var ::logic-var)
                :a keyword?
                :v (s/? (s/or :literal ::value, :logic-var ::logic-var)))
         (s/conformer (fn [triple]
                        (-> triple (update :src (some-fn identity (constantly '$)))))
                      identity)))

(s/def ::predicate
  (s/and vector?
         (-> (s/cat :application
                    (s/spec (-> (s/cat :f simple-symbol?
                                       :args (s/* (s/nonconforming (s/or :logic-var ::logic-var
                                                                         :value ::value))))
                                (s/nonconforming))))
             (s/nonconforming))
         (s/conformer first vector)))

(s/def ::where
  (s/coll-of (s/or :triple ::triple
                   :predicate ::predicate)))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::order-element (s/and vector?
                              (s/cat :find-arg (s/or :logic-var ::logic-var
                                                     :aggregate ::aggregate)
                                     :direction (s/? #{:asc :desc}))))

(s/def ::order-by (s/coll-of ::order-element :kind vector?))

(s/def ::query
  (s/keys :req-un [::find]
          :opt-un [::in ::where ::order-by ::offset ::limit]))

(s/def ::relation-arg
  (s/or :maps (s/coll-of (s/map-of simple-keyword? any?))
        :vecs (s/coll-of vector?)))

(defn wrap-predicates [plan predicates]
  (case (count predicates)
    0 plan
    1 [:select (first predicates) plan]
    [:select (list* 'and predicates) plan]))

(defn- conform-query [query]
  (let [conformed-query (s/conform ::query query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {::err/message "Malformed query"
                               :query query
                               :explain (s/explain-data ::query query)})))
    conformed-query))

(defn- ->table-keys [{in-bindings :in}]
  (->> in-bindings
       (into {} (keep (fn [[binding-type binding-arg]]
                        (case binding-type
                          :relation (MapEntry/create (set (first binding-arg))
                                                     (gensym "?in"))
                          :collection (MapEntry/create #{(first binding-arg)}
                                                       (gensym "?in"))
                          nil))))))

(defn- ->rel-expr [rel-clauses {:keys [in-scalars v-vars find-vars eid-srcs]}]
  (let [{[eid-type eid-arg] :e, :keys [src]} (first rel-clauses)

        eid-col (case eid-type
                  :literal {:col-name 'id,
                            :col-pred (list '= 'id eid-arg)}

                  :logic-var (let [retain-e-col? (or (contains? v-vars eid-arg)
                                                     (contains? find-vars eid-arg)
                                                     (> (count (get eid-srcs eid-arg)) 1))]
                               (when (or retain-e-col?
                                         (contains? in-scalars eid-arg))
                                 (cond-> {:col-name 'id}
                                   retain-e-col?
                                   (assoc :value-arg eid-arg)

                                   (contains? in-scalars eid-arg)
                                   (assoc :col-pred (list '= 'id eid-arg))))))

        cols (cond-> (for [[a clauses] (group-by :a rel-clauses)
                           :let [col-name (symbol (name a))]]
                       (reduce (fn [acc {[v-type v-arg] :v}]
                                 ;; TODO assumes at most one LV and at most one lit per EA
                                 (case v-type
                                   :literal (assoc acc :col-pred (list '= col-name v-arg))
                                   :logic-var (cond-> acc
                                                (contains? in-scalars v-arg)
                                                (assoc :col-pred (list '= col-name v-arg))

                                                :always
                                                (assoc :value-arg v-arg))
                                   nil acc))
                               {:col-name col-name}
                               clauses))
               eid-col (conj eid-col))

        vars (into #{} (keep :value-arg) cols)

        multi-col-predicates (for [unifed-vals (filter #(> (count %) 1) (vals (group-by :value-arg cols)))]
                               (list* '= (map :col-name unifed-vals)))]

    (-> [:project (vec vars)
         [:rename (->> cols
                       (into {} (comp (filter :value-arg)
                                      (map (juxt :col-name :value-arg)))))
          (cond-> [:scan
                   src
                   'xt_docs ;; assumes all docs put into system that
                   ;; want to be queried by datalog will be stored under xt_docs table
                   (-> (vec (for [{:keys [col-name col-pred]} cols]
                              (if col-pred
                                {col-name col-pred}
                                col-name)))
                       (conj '{application_time_start (<= application_time_start (current-timestamp))}
                             '{application_time_end (> application_time_end (current-timestamp))}))]
            ;; Needs to be done here as we rename unifying columns to the same column name just after this
            ;; I think we want to avoid duplicate column names, but thats a bigger task.
            (seq multi-col-predicates) (wrap-predicates multi-col-predicates))]]
        (with-meta {::vars vars}))))

(defn- join-exprs [left-expr right-expr]
  (let [{left-vars ::vars} (meta left-expr)
        {right-vars ::vars} (meta right-expr)
        vars (set/union left-vars right-vars)]
    (-> (if-let [[overlap-var & more-overlap-vars] (seq (set/intersection left-vars right-vars))]
          (if-not (seq more-overlap-vars)
            [:join [{overlap-var overlap-var}] left-expr right-expr]

            (let [var-mapping (->> (map (juxt identity gensym) more-overlap-vars)
                                   (into {}))]
              [:project (vec vars)
               [:select (reduce (fn [acc el]
                                  (list 'and acc el))
                                (for [[left right] var-mapping]
                                  (list '= left right)))
                [:join [{overlap-var overlap-var}]
                 left-expr
                 [:rename var-mapping right-expr]]]]))

          [:cross-join left-expr right-expr])

        (with-meta {::vars vars}))))

(defn- table-plan [src table-vars]
  (-> [:rename (->> table-vars
                    (into {} (map (juxt (comp symbol #(subs % 1) name) identity))))
       [:table src]]
      (with-meta {::vars table-vars})))

(defn- join-tables [expr table-keys]
  (let [expr-vars (::vars (meta expr))]
    (reduce (fn [[expr table-keys] [table-vars src :as entry]]
              (if (empty? (set/difference table-vars expr-vars))
                [(join-exprs (table-plan src table-vars) expr) table-keys]
                [expr (conj table-keys entry)]))
            [expr {}]
            table-keys)))

(defn- cross-join-tables [expr table-keys]
  (let [[expr table-keys] (if expr
                            [expr table-keys]
                            (when (seq table-keys)
                              (let [[[table-vars src] & more-table-keys] table-keys]
                                [(table-plan src table-vars) more-table-keys])))]
    ;; TODO this won't be _the_ most efficient way to join these tables
    ;; we'll want to cross-join the smallest one, and then see which ones then join, and repeat
    (reduce (fn [expr [table-vars src]]
              (join-exprs (table-plan src table-vars) expr))
            expr
            table-keys)))

(defn- compile-triples [triples {find-args :find, in-bindings :in} {:keys [table-keys]}]
  (let [find-vars (into #{} (map find-arg-var) find-args)
        in-scalars (into #{} (mapcat in-scalar-vars) in-bindings)
        xform->lvs (keep (fn [[var-type var-arg]]
                           (when (= :logic-var var-type)
                             var-arg)))
        v-vars (into #{} (comp (map :v) xform->lvs) triples)
        eid-srcs (->> (for [{[eid-type eid-arg] :e, :keys [src]} triples
                            :when (= eid-type :logic-var)]
                        (MapEntry/create eid-arg src))
                      (reduce (fn [acc [eid src]]
                                (-> acc (update eid (fnil conj #{}) src)))
                              {}))]
    (loop [[triple & more-triples] triples
           left-expr nil
           table-keys table-keys]
      (let [[left-expr table-keys] (join-tables left-expr table-keys)]
        (if-not triple
          (or (cross-join-tables left-expr table-keys)
              (throw (err/illegal-arg :no-clauses-available-to-query
                                      {::err/message "no clauses available to query"})))

          (let [same-src+entity? (comp #{[(:src triple) (:e triple)]} (juxt :src :e))
                clauses (cons triple (filter same-src+entity? more-triples))
                right-expr (->rel-expr clauses
                                       {:in-scalars in-scalars
                                        :find-vars find-vars
                                        :v-vars v-vars
                                        :eid-srcs eid-srcs})]

            (recur (remove same-src+entity? more-triples)
                   (if left-expr
                     (join-exprs left-expr right-expr)
                     right-expr)
                   table-keys)))))))

(defn compile-where [{where-clauses :where :as query} {:keys [table-keys]}]
  (let [{triples :triple,
         predicates :predicate} (->> where-clauses
                                     (reduce (fn [acc [clause-type clause-arg]]
                                               (update acc clause-type (fnil conj []) clause-arg))
                                             {}))]
    (-> (compile-triples triples query {:table-keys table-keys})
        (wrap-predicates predicates))))

(defn- aggregate-logic-var-name [{:keys [aggregate logic-var]}]
  (symbol (str aggregate "-" logic-var)))

(defn- with-group-by [plan {find-args :find}]
  (if (every? (comp #{:logic-var} first) find-args)
    [:project (mapv second find-args)
     plan]

    [:group-by (vec (for [[arg-type arg] find-args]
                      (case arg-type
                        :logic-var arg
                        :aggregate {(aggregate-logic-var-name arg)
                                    (let [{:keys [aggregate logic-var]} arg]
                                      (list aggregate logic-var))})))
     plan]))

(defn- with-order-by [plan {:keys [order-by]}]
  (if order-by
    [:order-by (vec (for [{:keys [find-arg direction]} order-by
                          :let [[arg-type arg] find-arg]]
                      [(case arg-type
                         :logic-var arg
                         :aggregate (aggregate-logic-var-name arg))
                       {:direction (or direction :asc)}]))
     plan]
    plan))

(defn- with-top [plan {:keys [limit offset]}]
  (if (or limit offset)
    [:top (cond-> {}
            offset (assoc :skip offset)
            limit (assoc :limit limit))
     plan]

    plan))

(defn- with-renamed-find-vars [plan {find-vars :find}]
  [:rename (->> find-vars
                (into {} (keep (fn [[var-type var-arg]]
                                 (when (= var-type :logic-var)
                                   (MapEntry/create var-arg (symbol (subs (name var-arg) 1))))))))
   plan])

(defn compile-query [query]
  (let [conformed-query (conform-query query)
        table-keys (->table-keys conformed-query)]
    {:conformed-query conformed-query
     :table-keys table-keys
     :plan (-> (compile-where conformed-query {:table-keys table-keys})
               (with-group-by conformed-query)
               (with-order-by conformed-query)
               (with-top conformed-query)
               (with-renamed-find-vars conformed-query))}))

(comment
  (compile-query '{:find [?e1 ?e2 ?a1 ?a2]
                   :in [$ [[?first ?last]]]
                   :where [[?e1 :name "Ivan"]
                           [?e2 :name "Ivan"]
                           [?e1 :age ?a1]
                           [?e2 :age ?a2]]}))

(defn- split-args [args {:keys [table-keys], {in-bindings :in} :conformed-query}]
  (when (not= (count in-bindings) (count args))
    (throw (err/illegal-arg :in-arity-exception {::err/message ":in arity mismatch"
                                                 :expected (s/unform ::in in-bindings)
                                                 :actual (count args)})))

  {:params (->> (mapcat (fn [[binding-type binding-arg] arg]
                          (case binding-type
                            (:source :scalar) [(MapEntry/create binding-arg arg)]
                            :tuple (map (fn [logic-var arg]
                                          (MapEntry/create logic-var arg))
                                        binding-arg
                                        arg)
                            (:collection :relation) nil))
                        in-bindings
                        args)
                (into {}))

   :table-args (->> (mapcat (fn [[binding-type binding-arg] arg]
                              (case binding-type
                                (:source :scalar :tuple) nil

                                :collection (let [binding (first binding-arg)
                                                  binding-k (-> binding name (subs 1) keyword)]
                                              (if-not (coll? arg)
                                                (throw (err/illegal-arg :bad-collection
                                                                        {:binding binding
                                                                         :coll arg}))
                                                [(MapEntry/create (get table-keys #{binding})
                                                                  (vec (for [v arg]
                                                                         {binding-k v})))]))

                                :relation (let [conformed-arg (s/conform ::relation-arg arg)]
                                            (if (s/invalid? conformed-arg)
                                              (throw (err/illegal-arg :bad-relation
                                                                      {:binding (first binding-arg)
                                                                       :relation arg
                                                                       :explain-data (s/explain-data ::relation-arg arg)}))
                                              (let [[rel-type rel] conformed-arg
                                                    binding (first binding-arg)]
                                                [(MapEntry/create (get table-keys (set binding))
                                                                  (case rel-type
                                                                    :maps rel
                                                                    :vecs (mapv #(zipmap (mapv keyword binding) %) rel)))])))))
                            in-bindings
                            args)
                    (into {}))})

(defn open-datalog-query ^core2.IResultSet [^BufferAllocator allocator query db args]
  (let [{:keys [plan], :as compiled-query} (compile-query (dissoc query :basis :basis-timeout :default-tz))

        pq (op/prepare-ra plan)

        {:keys [params table-args]} (split-args args compiled-query)

        ^AutoCloseable
        params (vw/open-params allocator params)]
    (try
      (-> (.bind pq {:srcs {'$ db}, :params params, :table-args table-args
                     :current-time (get-in query [:basis :current-time])
                     :default-tz (:default-tz query)})
          (.openCursor)
          (op/cursor->result-set params))
      (catch Throwable t
        (.close params)
        (throw t)))))
