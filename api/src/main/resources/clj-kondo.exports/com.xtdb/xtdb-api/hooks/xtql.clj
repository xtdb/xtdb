(ns ^:no-doc hooks.xtql
  (:require [clj-kondo.hooks-api :as api]))

(def source-op?
  #{'from 'rel 'unify})

(def tail-op?
  #{'aggregate
    'limit 'offset
    'where 
    'order-by 
    'with 'without 'return 
    'unnest})

(def unify-clause?
  #{'from 'rel
    'join 'left-join
    'unnest
    'where
    'with})

(defn node-map? [node]
  (contains? #{:map :namespaced-map}
             (:tag node)))

(defn node-namespaced-map? [node]
  (= :namespaced-map (:tag node)))

(defn map-children [node]
  (->> (if (node-namespaced-map? node)
          (-> node :children first)
          node)
       :children
       (partition-all 2)))

(defn node-vector? [node]
  (= :vector (:tag node)))

(defn node-list? [node]
  (= :list (:tag node)))

(defn node-symbol? [node]
  (symbol? (:value node)))

(defn node-symbol [node]
  (:value node))

(defn node-keyword? [node]
  (keyword? (:k node)))

(defn node-keyword [node]
  (:k node))

(defn node-quote? [node]
  (= :quote (:tag node)))

(defn node-op [node]
  (-> node :children first))

(declare lint-query)

(defmulti lint-unify-clause #(-> % node-op node-symbol))
(defmulti lint-source-op #(-> % node-op node-symbol))
(defmulti lint-tail-op #(-> % node-op node-symbol))

(defn lint-not-arg-symbol [node]
  (when (= \$ (-> node node-symbol str first))
    (api/reg-finding!
      (assoc (meta node)
             :message "unexpected parameter in binding"
             :type :xtql/unrecognized-parameter))))

(defn lint-bind [node]
  (cond
    (node-symbol? node)
    ;; TODO: Make own type, should really be a warning
    (lint-not-arg-symbol node)

    (node-map? node)
    (doseq [[k _v] (map-children node)]
      (when-not (node-keyword? k)
        (api/reg-finding!
          (assoc (meta k)
                 :message "all keys in binding maps must be keywords"
                 :type :xtql/type-mismatch))))

    :else
    (api/reg-finding!
      (assoc (meta node)
             :message "expected a symbol or map"
             :type :xtql/type-mismatch))))

;; TODO: Lint more unify clauses
(defmethod lint-unify-clause :default [node]
  (when-not (unify-clause? (-> node node-op node-symbol))
    (api/reg-finding!
      (assoc (some-> node :children first meta)
             :message "unrecognized unify clause"
             :type :xtql/unrecognized-operation))))

(defmethod lint-unify-clause 'from [node]
  (lint-source-op node))

(defmethod lint-unify-clause 'rel [node]
  (lint-source-op node))

(defmethod lint-unify-clause 'with [node]
  (let [opts (-> node :children rest)]
    (when-not (>= (count opts) 1)
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at least one argument"
               :type :xtql/invalid-arity)))
    (doseq [opt opts]
      (if (node-map? opt)
        (let [ks (->> opt
                      map-children
                      (map first)
                      (remove node-symbol?))]
          (doseq [k ks]
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all keys to be symbols in a unify"
                     :type :xtql/type-mismatch))))
        (api/reg-finding!
          (assoc (meta opt)
                 :message "opts must be a map"
                 :type :xtql/type-mismatch))))))

(defn lint-join-clause [node]
  (let [args (-> node :children rest)]
    (if-not (= (count args) 2)
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at exactly two arguments"
               :type :xtql/invalid-arity))
      (let [[query opts] args]
        (lint-query query)
        (cond
          (node-vector? opts)
          (->> opts :children (run! lint-bind))
          (node-map? opts)
          (let [kvs (map-children opts)
                ks (->> kvs
                        (map first)
                        (map node-keyword)
                        (remove nil?)
                        (into #{}))]
            (when-not (contains? ks :bind)
              (api/reg-finding!
                (assoc (meta opts)
                       :message "Missing :bind parameter"
                       :type :xtql/missing-parameter)))
            (doseq [[k v] kvs]
              (when-not (node-keyword? k)
                (api/reg-finding!
                  (assoc (meta k)
                         :message "All keys in 'opts' must be keywords"
                         :type :xtql/type-mismatch)))
              (case (node-keyword k)
                :bind (if (node-vector? v)
                        (->> v :children (run! lint-bind))
                        (api/reg-finding!
                          (assoc (meta v)
                                 :message "expected :bind value to be a vector"
                                 :type :xtql/type-mismatch)))
                :args (if (node-vector? v)
                        ;; TODO: Make args specific
                        (->> v :children (run! lint-bind))
                        (api/reg-finding!
                          (assoc (meta v)
                                 :message "expected :args value to be a vector"
                                 :type :xtql/type-mismatch)))
                ; else
                (api/reg-finding!
                  (assoc (meta k)
                         :message "unrecognized parameter"
                         :type :xtql/unrecognized-parameter)))))
          :else
          (api/reg-finding!
            (assoc (meta node)
                   :message "opts must be a map or vector"
                   :type :xtql/type-mismatch)))))))

(defmethod lint-unify-clause 'join [node]
  (lint-join-clause node))

(defmethod lint-unify-clause 'inner-join [node]
  (lint-join-clause node))

(defmethod lint-unify-clause 'unnest [node]
  (let [opts (-> node :children rest)]
    (when-not (= 1 (count opts))
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at exactly one argument"
               :type :xtql/invalid-arity)))
    (let [opt (first opts)]
      (if (node-map? opt)
        (doseq [[k _v] (map-children opt)]
          (when-not (node-symbol? k)
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all columns to be symbols"
                     :type :xtql/type-mismatch))))
        (api/reg-finding!
          (assoc (meta opt)
                 :message "expected opt to be a map"
                 :type :xtql/type-mismatch))))))


(defmethod lint-source-op :default [node]
  (let [op (-> node node-op node-symbol)]
    (if (tail-op? op)
      (api/reg-finding!
        (assoc (some-> node :children first meta)
               :message "tail op in source position"
               :type :xtql/unrecognized-operation))
      (when-not (source-op? op)
        (api/reg-finding!
          (assoc (some-> node :children first meta)
                 :message "unrecognized source operation"
                 :type :xtql/unrecognized-operation))))))

(defmethod lint-source-op 'from [node]
  (let [[_ table opts] (some-> node :children)]
    (when-not (node-keyword? table)
      (api/reg-finding!
        (assoc (meta table)
               :message "expected 'table' to be a keyword"
               :type :xtql/type-mismatch)))
    (case (:tag opts)
      :vector (->> opts :children (run! lint-bind))
      :map 
      (let [kvs (map-children opts)
            ks (->> kvs
                    (map first)
                    (map node-keyword)
                    (remove nil?)
                    (into #{}))]
        (when-not (contains? ks :bind)
          (api/reg-finding!
            (assoc (meta opts)
                   :message "Missing :bind parameter"
                   :type :xtql/missing-parameter)))
        (doseq [[k v] kvs]
          (when-not (node-keyword? k)
            (api/reg-finding!
              (assoc (meta k)
                     :message "All keys in 'opts' must be keywords"
                     :type :xtql/type-mismatch)))
          (case (node-keyword k)
            :bind (if (node-vector? v)
                    (->> v :children (run! lint-bind))
                    (api/reg-finding!
                      (assoc (meta opts)
                             :message "expected :bind value to be a vector"
                             :type :xtql/type-mismatch)))
            ;; TODO
            :for-valid-time nil
            ;; TODO
            :for-system-time nil
            ; else
            (api/reg-finding!
              (assoc (meta k)
                     :message "unrecognized parameter"
                     :type :xtql/unrecognized-parameter)))))
      (api/reg-finding!
        (assoc (meta opts)
               :message "expected 'opts' to be either a map or vector"
               :type :xtql/type-mismatch)))))

(defmethod lint-source-op 'unify [node]
  (let [[_ & clauses] (some-> node :children)]
    (doseq [bad-op (remove node-list? clauses)]
      (api/reg-finding!
        (assoc (meta bad-op)
               :message "all operations in a unify must be lists"
               :type :xtql/type-mismatch)))
    (when (= (count clauses) 1)
      (let [clause (first clauses)
            clause-op (-> clause node-op node-symbol)
            unify-node (some-> node :children first)]
        (case clause-op
          from (api/reg-finding!
                 (assoc (meta unify-node)
                        :message "redundant unify"
                        :type :xtql/redundant-unify))
          rel (api/reg-finding!
                (assoc (meta unify-node)
                       :message "redundant unify"
                       :type :xtql/redundant-unify))
          ;; TODO: Cover other operators
          nil)))
    (->> clauses
         (filter node-list?)
         (run! lint-unify-clause))))

(defmethod lint-source-op 'rel [node]
  (let [[_ _expr binds] (some-> node :children)]
    (if (node-vector? binds)
      (->> binds :children (run! lint-bind))
      (api/reg-finding!
        (assoc (meta binds)
               :message "expected rel binding to be a vector"
               :type :xtql/type-mismatch)))))

;; TODO: Lint more tail ops
(defmethod lint-tail-op :default [node]
  (let [op (-> node node-op node-symbol)]
    (if (source-op? op)
      (api/reg-finding!
        (assoc (some-> node :children first meta)
               :message "source op in tail position"
               :type :xtql/unrecognized-operation))
      (when-not (tail-op? op)
        (api/reg-finding!
          (assoc (some-> node :children first meta)
                 :message "unrecognized tail operation"
                 :type :xtql/unrecognized-operation))))))

(defn lint-keyword [node name]
  (when-not (node-keyword? node)
    (api/reg-finding!
      (assoc (meta node)
             :message (str "expected '" name "' to be a keyword")
             :type :xtql/type-mismatch))))

(defn lint-enum [node name values]
  ;; TODO: Expand to more than just keywords?
  ;;       Maybe a `node-value` function?
  (when-not (contains? values (node-keyword node))
    (api/reg-finding!
      (assoc (meta node)
             :message (str "expected '" name "' to be one of " values)
             ;; TODO: change to different type?
             :type :xtql/type-mismatch))))

(defmethod lint-tail-op 'limit [node]
  (let [opts (-> node :children rest)]
    (when-not (= 1 (count opts))
      (api/reg-finding!
        (assoc (meta node)
               :message "expected exactly one argument"
               :type :xtql/invalid-arity)))
    (when-let [opt (first opts)]
      (when-not (some-> opt :value int?)
        (api/reg-finding!
          (assoc (meta opt)
                 :message "expected limit to be an integer"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'offset [node]
  (let [opts (-> node :children rest)]
    (when-not (= 1 (count opts))
      (api/reg-finding!
        (assoc (meta node)
               :message "expected exactly one argument"
               :type :xtql/invalid-arity)))
    (when-let [opt (first opts)]
      (when-not (some-> opt :value int?)
        (api/reg-finding!
          (assoc (meta opt)
                 :message "expected offset to be an integer"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'with [node]
  (let [opts (-> node :children rest)]
    (when-not (>= (count opts) 1)
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at least one argument"
               :type :xtql/invalid-arity)))
    (doseq [opt opts]
      (cond
        (node-symbol? opt)
        (lint-not-arg-symbol opt)
        (node-map? opt)
        (let [ks (->> opt
                      map-children
                      (map first)
                      (remove node-keyword?))]
          (doseq [k ks]
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all keys to be keywords"
                     :type :xtql/type-mismatch))))
        :else
        (api/reg-finding!
          (assoc (meta opt)
                 :message "opts must be a symbol or map"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'return [node]
  (let [opts (-> node :children rest)]
    (when-not (>= (count opts) 1)
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at least one argument"
               :type :xtql/invalid-arity)))
    (doseq [opt opts]
      (cond
        (node-symbol? opt)
        (lint-not-arg-symbol opt)
        (node-map? opt)
        (let [ks (->> opt
                      map-children
                      (map first)
                      (remove node-keyword?))]
          (doseq [k ks]
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all keys to be keywords"
                     :type :xtql/type-mismatch))))
        :else
        (api/reg-finding!
          (assoc (meta opt)
                 :message "opts must be a symbol or map"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'order-by [node]
  (doseq [opt (-> node :children rest)]
    (cond
      (node-symbol? opt)
      (lint-not-arg-symbol opt)

      (node-map? opt)
      (let [kvs (map-children opt)
            ks (->> kvs
                    (map first)
                    (map node-keyword)
                    (remove nil?)
                    (into #{}))]
        (when-not (contains? ks :val)
          (api/reg-finding!
            (assoc (meta opt)
                   :message "Missing :val parameter"
                   :type :xtql/missing-parameter)))
        (doseq [[k v] kvs]
          (when-not (node-keyword? k)
            (api/reg-finding!
              (assoc (meta k)
                     :message "All keys in 'opts' must be keywords"
                     :type :xtql/type-mismatch)))
          (case (node-keyword k)
            :val
            (cond
              (node-symbol? v)
              (lint-not-arg-symbol v)
              (node-keyword? v)
              (api/reg-finding!
                (assoc (meta v)
                       :message "expected :val value to be a symbol or an expression"
                       :type :xtql/type-mismatch)))
              ; else do nothing
            :dir
            (if (node-keyword? v)
              (lint-enum v :dir #{:asc :desc})
              (lint-keyword v ":dir value"))
            :nulls 
            (if (node-keyword? v)
              (lint-enum v :nulls #{:first :last})
              (lint-keyword v ":nulls value"))
            ; else
            (api/reg-finding!
              (assoc (meta k)
                     :message "unrecognized parameter"
                     :type :xtql/unrecognized-parameter)))))

      :else
      (api/reg-finding!
        (assoc (meta opt)
               :message "opts must be a symbol or map"
               :type :xtql/type-mismatch)))))

(defmethod lint-tail-op 'without [node]
  (let [columns (-> node :children rest)]
    (when-not (>= (count columns) 1)
      ;; TODO: Should be a warning really
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at least one column"
               :type :xtql/invalid-arity)))
    (doseq [column columns]
      (when-not (node-keyword? column)
        (api/reg-finding!
          (assoc (meta column)
                 :message "expected column to be a keyword"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'aggregate [node]
  (let [opts (-> node :children rest)]
    (when-not (>= (count opts) 1)
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at least one argument"
               :type :xtql/invalid-arity)))
    (doseq [opt opts]
      (cond
        (node-symbol? opt)
        (lint-not-arg-symbol opt)
        (node-map? opt)
        (doseq [[k _v] (map-children opt)]
          (when-not (node-keyword? k)
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all keys to be keywords"
                     :type :xtql/type-mismatch))))

        :else
        (api/reg-finding!
          (assoc (meta opt)
                 :message "expected opts to be a symbol or map"
                 :type :xtql/type-mismatch))))))

(defmethod lint-tail-op 'unnest [node]
  (let [opts (-> node :children rest)]
    (when-not (= 1 (count opts))
      (api/reg-finding!
        (assoc (meta node)
               :message "expected at exactly one argument"
               :type :xtql/invalid-arity)))
    (let [opt (first opts)]
      (if (node-map? opt)
        (doseq [[k _v] (map-children opt)]
          (when-not (node-keyword? k)
            (api/reg-finding!
              (assoc (meta k)
                     :message "expected all columns to be keywords"
                     :type :xtql/type-mismatch))))
        (api/reg-finding!
          (assoc (meta opt)
                 :message "expected opt to be a map"
                 :type :xtql/type-mismatch))))))

(defn lint-pipeline [node]
  (let [[_ & ops] (some-> node :children)]
    (doseq [bad-op (remove node-list? ops)]
      (api/reg-finding!
        (assoc (meta bad-op)
               :message "all operations in a pipeline must be lists"
               :type :xtql/type-mismatch)))
    (when (= 1 (count ops))
      (api/reg-finding!
        (assoc (-> node :children first meta)
               :message "redundant pipeline"
               :type :xtql/redundant-pipeline)))
    (let [first-op (first ops)]
      (when (node-list? first-op)
        (lint-source-op (first ops))))
    (->> ops
         (drop 1)
         (filter node-list?)
         (run! lint-tail-op))))

(defn lint-query [node]
  (if (= '-> (node-symbol (-> node :children first)))
    (lint-pipeline node)
    (lint-source-op node)))

;; TODO: Lint other functions that take queries

(defn q [{:keys [node]}]
  (let [[_ _node quoted-query] (some-> node :children)]
    (when (node-quote? quoted-query)
      (let [query (-> quoted-query :children first)]
        (lint-query query)))))
