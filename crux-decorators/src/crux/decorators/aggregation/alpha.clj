(ns crux.decorators.aggregation.alpha
  (:require [crux.api :as api]
            [crux.decorators.core :as decorators]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

(defn variable?
  [s]
  (and (symbol? s) (= \? (first (name s)))))

(s/def ::partition-by (s/coll-of variable?))
(s/def ::vector-aggregation-expr any?)
(s/def ::list-aggregation-expr any?)
(s/def ::aggregation-expr (s/or :vector ::vector-aggregation-expr
                                :list ::list-aggregation-expr))
(s/def ::select (s/map-of variable? ::aggregation-expr))
(s/def ::aggr (s/keys :req-un [::partition-by ::select]))

(defn find-used-variables
  [expr]
  (let [variables (atom [])]
    (walk/prewalk
      (fn [form]
        (when (variable? form)
          (swap! variables conj form))
        form)
      expr)
    @variables))

(defn unique
  [coll]
  (loop [seen #{}
         res []
         [f & r :as coll] coll]
    (if (empty? coll)
      res
      (if (seen f)
        (recur seen res r)
        (recur (conj seen f) (conj res f) r)))))

(defn aggregation-variables
  [query]
  (->> (concat
         (->> query :aggr :partition-by)
         (->> query :aggr :select vals (mapcat find-used-variables)))
       (unique)))

(defn query-order
  [query]
  (vec
    (for [item (->> :aggr query :partition-by)]
      [item :asc])))

(defn variable->keyword
  [variable]
  (keyword (subs (name variable) 1)))

(defn compile-reducer
  [expr query-variables]
  (cond
    (vector? expr)
    (let [[seed-expr progress-expr & extra-required-variables] expr

          input-vars-s (gensym 'input-vars-s)
          bindings (vec (interleave
                          query-variables
                          (for [i (range (count query-variables))]
                            `(get ~input-vars-s ~i))))]
      (eval
        `(fn []
           (fn
             ([] ~seed-expr)
             ([ac#] ac#)
             ([ac# ~input-vars-s]
              (let ~bindings (let [~'acc ac#] ~progress-expr)))))))

    (list? expr)
    (do
      (assert (list? expr))
      (assert (= 2 (count expr)))
      (let [reducer-f (first expr)
            input-expr (second expr)
            input-vars-s (gensym 'input-vars-s)
            bindings (vec (interleave
                            query-variables
                            (for [i (range (count query-variables))]
                              `(get ~input-vars-s ~i))))]
        (eval
          `(fn []
             (let [rf# ~reducer-f]
               (fn
                 ([] (rf#))
                 ([ac#] (rf# ac#))
                 ([ac# ~input-vars-s]
                  (let ~bindings (rf# ac# ~input-expr)))))))))))

(defn running-plan
  [query]
  (let [aggregation-variables (aggregation-variables query)]
    {:query-variables aggregation-variables
     :output-names (vec (map variable->keyword aggregation-variables))
     :partition-segments-count (->> query :aggr :partition-by count)
     :aggregators (into
                    {}
                    (for [[target-var expr] (-> query :aggr :select)]
                      [target-var (compile-reducer expr aggregation-variables)]))}))

(defn decorated-query
  [query]
  (-> (dissoc query :aggr)
      (assoc :order-by (query-order query))
      (assoc :find (aggregation-variables query))))

(defn run-plan
  [db snapshot
   {:keys [partition-segments-count aggregators query-variables output-names] :as plan}
   decorated-query xform rf]
  (let [rfi (rf)]
    (loop [acc (rfi)
           active-partition nil
           partition-aggregators nil
           [f & r :as result] (api/q db snapshot decorated-query)]
      (letfn [(complete-partition [acc partition-aggregators]
                (if partition-aggregators
                  (rfi acc (into
                             {}
                             (concat
                               (for [i (range partition-segments-count)]
                                 [(get output-names i)
                                  (get active-partition i)])
                               (for [[k {:keys [rf acc]}] partition-aggregators]
                                 [(variable->keyword k) (rf acc)]))))
                  acc))
              (process-input [partition-aggregators]
                (into
                  {}
                  (for [[k {:keys [rf acc]}] partition-aggregators]
                    [k {:rf rf :acc (rf acc f)}])))]
        (if (empty? result)
          (rfi (complete-partition acc partition-aggregators))
          (let [partition (vec (take partition-segments-count f))]
            (if (not= partition active-partition)
              (let [acc (complete-partition acc partition-aggregators)
                    new-partition-aggregators (into {} (for [[k fun] aggregators]
                                                         (let [rf (fun)]
                                                           [k {:rf rf :acc (rf)}])))]
                (recur acc partition (process-input new-partition-aggregators) r))
              (recur acc partition (process-input partition-aggregators) r))))))))

(defn q
  ([db query]
   (if (:aggr query)
     (with-open [s (api/new-snapshot db)]
       (vec (q db s query)))
     (api/q db query)))
  ([db snapshot query]
   (if (:aggr query)
     (do
       (s/assert ::aggr (:aggr query))
       (run-plan
         db snapshot (running-plan query)
         (decorated-query query)
         identity
         (constantly conj)))
     (api/q db snapshot query))))

(def aggregation-decorator
  (decorators/node-decorator {:data-source {#'api/q q}}))
