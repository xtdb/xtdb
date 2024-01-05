(ns hooks.xtql
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

(defmulti lint-unify-clause (fn [node] (-> node api/sexpr first)))
(defmulti lint-source-op (fn [node] (-> node api/sexpr first)))
(defmulti lint-tail-op (fn [node] (-> node api/sexpr first)))

;; TODO: Lint more unify clauses
(defmethod lint-unify-clause :default [node]
  (let [sexpr (api/sexpr node)
        op (first sexpr)]
    (when-not (unify-clause? op)
      (api/reg-finding!
        (assoc (some-> node :children first meta)
               :message "unrecognized unify clause"
               :type :xtql/unrecognized-operation)))))

(defmethod lint-unify-clause 'from [node]
  (lint-source-op node))

;; TODO: Lint more source ops
(defmethod lint-source-op :default [node]
  (let [sexpr (api/sexpr node)
        op (first sexpr)]
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

(defn lint-bind [node]
  (let [sexpr (api/sexpr node)]
    (cond
      (symbol? sexpr)
      (when (= \$ (first (str sexpr)))
        (api/reg-finding!
          ;; TODO: Make own type, should really be a warning
          (assoc (meta node)
                 :message "unexpected parameter in binding"
                 :type :xtql/unrecognized-parameter)))

      (map? sexpr)
      (doseq [[k _v] (->> (if (= (:tag node) :namespaced-map)
                            (-> node :children first)
                            node)
                          :children
                          (partition-all 2))]
        (when-not (keyword? (api/sexpr k))
          (api/reg-finding!
            (assoc (meta k)
                   :message "all keys in binding maps must be keywords"
                   :type :xtql/type-mismatch))))

      :else
      (api/reg-finding!
        (assoc (meta node)
               :message "expected a symbol or map"
               :type :xtql/type-mismatch)))))

(defmethod lint-source-op 'from [node]
  (let [[_ table opts] (some-> node :children)]
    (when-not (keyword? (api/sexpr table))
      (api/reg-finding!
        (assoc (meta table)
               :message "expected 'table' to be a keyword"
               :type :xtql/type-mismatch)))
    (case (:tag opts)
      :vector (->> opts :children (run! lint-bind))
      :map 
      (let [kvs (some->> opts :children (partition-all 2))
            ks (map first kvs)]
        (when-not (contains? (->> ks
                                  (map api/sexpr)
                                  (filter keyword?)
                                  (into #{}))
                             :bind)
          (api/reg-finding!
            (assoc (meta opts)
                   :message "Missing :bind parameter"
                   :type :xtql/missing-parameter)))
        (doseq [[k v] (some->> opts :children (partition-all 2))]
          (when-not (keyword? (api/sexpr k))
            (api/reg-finding!
              (assoc (meta k)
                     :message "All keys in 'opts' must be keywords"
                     :type :xtql/type-mismatch)))
          (case (api/sexpr k)
            :bind (if (= (:tag v) :vector)
                    (->> v :children (run! lint-bind))
                    (api/reg-finding!
                      (assoc (meta opts)
                             :message "expected :bind value to be a vector"
                             :type :xtql/type-mismatch)))
            ;; TODO
            :for-valid-time nil
            ;; TODO
            :for-system-time nil
            (api/reg-finding!
              (assoc (meta k)
                     :message "unrecognized parameter"
                     :type :xtql/unrecognized-parameter)))))
      (api/reg-finding!
        (assoc (meta opts)
               :message "expected 'opts' to be either a map or vector"
               :type :xtql/type-mismatch)))))

(defmethod lint-source-op 'unify [node]
  (let [clauses (some-> node :children rest)]
    (doseq [bad-op (remove #(= (:tag %) :list)  clauses)]
      (api/reg-finding!
        (assoc (meta bad-op)
               :message "all operations in a unify must be lists"
               :type :xtql/type-mismatch)))
    (when (= (count clauses) 1)
      (let [clause (first clauses)
            clause-op (-> clause api/sexpr first)
            unify-node (some-> node :children first)]
        (case clause-op
          from (api/reg-finding!
                 (assoc (meta unify-node)
                        :message "redundant unify"
                        :type :xtql/redundant-unify))
          rel (do
                (println clause-op)
                (api/reg-finding!
                  (assoc (meta unify-node)
                         :message "redundant unify"
                         :type :xtql/redundant-unify)))
          ;; TODO: Cover other operators
          ;; Already covered in lint-unify-cluse
          nil)))
      ;; What cases?
      ;; Single source op, in which case warn
      ;; Single unify clause that isn't source op
    (run! lint-unify-clause
          (filter #(= (:tag %) :list) clauses))))

;; TODO: Lint more tail ops
(defmethod lint-tail-op :default [node]
  (let [sexpr (api/sexpr node)
        op (first sexpr)]
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

(defn lint-pipeline [node]
  (let [ops (some-> node :children rest)]
    (doseq [bad-op (remove #(= (:tag %) :list) ops)]
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
      (when (= (:tag first-op) :list)
        (lint-source-op (first ops))))
    (run! lint-tail-op (->> ops
                            (drop 1)
                            (filter #(= (:tag %) :list))))))

(defn lint-query [node]
  (let [quoted-query (api/sexpr node)]
    (when (= 'quote (first quoted-query))
      (let [query (second quoted-query)
            query-node (some-> node :children first)]
        (if (= '-> (first query))
          (lint-pipeline query-node)
          (lint-source-op query-node))))))

;; TODO: Lint other functions that take queries

(defn q [{:keys [node]}]
  ;; TODO: Lint other params
  (let [query (some-> node :children (nth 2))]
    (when (= (:tag query) :quote)
      (lint-query query))))
