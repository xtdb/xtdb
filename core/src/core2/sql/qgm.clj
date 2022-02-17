(ns core2.sql.qgm
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]))

;; Query Graph Model using internal triple store.
;; http://projectsweb.cs.washington.edu/research/projects/db/weld/pirahesh-starburst-92.pdf

;; TODO: try constructing this by adding the triples local for each
;; node during a collect somehow?

(s/def :db/id symbol?)
(s/def :qgm/id :db/id)

(s/def :qgm.box/type #{:qgm.box.type/base-table :qgm.box.type/select})

(defmulti box-spec :qgm.box/type)

(s/def :qgm/box (s/multi-spec box-spec :qgm.box/type))

(s/def :qgm.box.base-table/name symbol?)

(defmethod box-spec :qgm.box.type/base-table [_]
  (s/keys :req [:db/id :qgm.box/type :qgm.box.base-table/name]))

(s/def :qgm.box.head/distinct? boolean?)
(s/def :qgm.box.head/columns (s/coll-of symbol? :kind vector :min-count 1))
(s/def :qgm.box.body/columns (s/coll-of any? :kind vector :min-count 1))
(s/def :qgm.box.body/distinct #{:qgm.box.body.distinct/enforce
                                :qgm.box.body.distinct/preserve
                                :qgm.box.body.distinct/permit})
(s/def :qgm.box.body/quantifiers (s/coll-of :qgm/id :kind vector :min-count 1))

(defmethod box-spec :qgm.box.type/select [_]
  (s/keys :req [:db/id :qgm.box/type
                :qgm.box.head/distinct? :qgm.box.head/columns
                :qgm.box.body/columns :qgm.box.body/distinct :qgm.box.body/quantifiers]))

(s/def :qgm.quantifier/type #{:qgm.quantifier.type/foreach
                              :qgm.quantifier.type/preserved-foreach
                              :qgm.quantifier.type/existential
                              :qgm.quantifier.type/all})
(s/def :qgm.quantifier/columns (s/coll-of symbol? :kind vector? :min-count 1))
(s/def :qgm.quantifier/ranges-over :qgm/id)

(s/def :qgm/quantifier (s/keys :req [:db/id :qgm.quantifier/type :qgm.quantifier/columns :qgm.quantifier/ranges-over]))

(s/def :qgm.predicate/expression any?)
(s/def :qgm.predicate/quantifiers (s/coll-of :qgm/id :kind set?))
(s/def :qgm/predicate (s/keys :req [:db/id :qgm.predicate/expression :qgm.predicate/quantifiers]))

(s/def :qgm/node (s/or :box :qgm/box :quantifier :qgm/quantifier :predicate :qgm/predicate))
(s/def :qgm/graph (s/coll-of :qgm/node :kind set?))

;; Internal triple store.

(s/def :db/tx-op (s/or :entity (s/and (s/map-of keyword? any?)
                                      (s/keys :req [:db/id]))
                       :add (s/cat :op #{:db/add} :e :db/id :a keyword? :v any?)
                       :retract (s/cat :op #{:db/retract} :e :db/id :a keyword? :v any?)
                       :retract-entity (s/cat :op #{:db/retractEntity} :e :db/id)))

(defn- add-triple [db [e a v]]
  (let [conj' (fnil conj #{})]
    (-> db
        (update-in [:eav e a] conj' v)
        (update-in [:aev a e] conj' v)
        (update-in [:ave a v] conj' e)
        (update-in [:vae v a] conj' e))))

(defn- retract-triple [db [e a v]]
  (-> db
      (update-in [:eav e a] disj v)
      (update-in [:aev a e] disj v)
      (update-in [:ave a v] disj e)
      (update-in [:vae v a] disj e)))

(defn- entity-triples [{:keys [eav] :as db} eid]
   (for [[a vs] (get eav eid)
         v vs]
     [eid a v]))

(defn- retract-entity [db eid]
  (reduce
   retract-triple
   db
   (entity-triples db eid)))

(defn- entity->triples [entity]
  (let [e (:db/id entity)]
    (for [[a v] entity
          v (if (set? v)
              v
              #{v})]
      [e a v])))

(defn- add-entity [db entity]
  (reduce add-triple db (entity->triples entity)))

(defn datoms [db index & [x y z :as components]]
  (if-let [idx (get db index)]
    (let [ks (map (comp keyword str) (seq (name index)))]
      (case (count components)
        0 (for [[x ys] idx
                [y zs] ys
                z zs]
            (zipmap ks [x y z]))
        1 (for [[y zs] (get-in idx components)
                z zs]
            (zipmap ks [x y z]))
        2 (for [z (get-in idx components)]
            (zipmap ks [x y z]))
        3 (when (get-in idx components)
            [(zipmap ks (vec components))])))
    (throw (IllegalArgumentException. "unknown index: " index))))

(defn entity [db eid]
  (reduce
   (fn [acc {:keys [e a v]}]
     (update acc a (fn [x]
                     (cond
                       (set? x)
                       (conj x v)

                       (some? x)
                       (conj #{} x v)

                       :else
                       v))))
   {}
   (datoms db :eav eid)))

(defn transact [db tx-ops]
  (reduce
   (fn [db tx-op]
     (s/assert :db/tx-op tx-op)
     (let [[op-type {:keys [e a v] :as tx-op}] (s/conform :db/tx-op tx-op)]
       (case op-type
         :entity (add-entity db tx-op)
         :db/retractEntity (retract-entity db e)
         :db/add (add-triple db [e a v])
         :db/retract (retract-triple db [e a v]))))
   db
   tx-ops))

(comment

  (let [db (->> '[{:db/id b1
                   :qgm.box/type :qgm.box.type/base-table
                   :qgm.box.base-table/name inventory}
                  {:db/id b2
                   :qgm.box/type :qgm.box.type/base-table
                   :qgm.box.base-table/name quotations}
                  {:db/id b3
                   :qgm.box/type :qgm.box.type/select
                   :qgm.box.head/distinct? true
                   :qgm.box.head/columns [partno descr suppno]
                   :qgm.box.body/columns [q1.partno q1.descr q2.suppno]
                   :qgm.box.body/distinct :qgm.box.body.distinct/enforce
                   :qgm.box.body/quantifiers #{q1 q2 q4}}
                  {:db/id b4
                   :qgm.box/type :qgm.box.type/select
                   :qgm.box.head/distinct? false
                   :qgm.box.head/columns [price]
                   :qgm.box.body/columns [q3.price]
                   :qgm.box.body/distinct :qgm.box.body.distinct/permit
                   :qgm.box.body/quantifiers #{q3}}

                  {:db/id q1
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno descr]
                   :qgm.quantifier/ranges-over box1}
                  {:db/id q2
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno price]
                   :qgm.quantifier/ranges-over box2}
                  {:db/id q3
                   :qgm.quantifier/type :qgm.quantifier.type/foreach
                   :qgm.quantifier/columns [partno price]
                   :qgm.quantifier/ranges-over box2}
                  {:db/id q4
                   :qgm.quantifier/type :qgm.quantifier.type/all
                   :qgm.quantifier/columns [price]
                   :qgm.quantifier/ranges-over box4}

                  {:db/id p1
                   :qgm.predicate/expression (= q1.descr "engine")
                   :qgm.predicate/quantifiers #{q1}}
                  {:db/id p2
                   :qgm.predicate/expression (= q1.partno q2.partno)
                   :qgm.predicate/quantifiers #{q1 q2}}
                  {:db/id p3
                   :qgm.predicate/expression (<= q2.partno q4.partno)
                   :qgm.predicate/quantifiers #{q2 q4}}
                  {:db/id p4
                   :qgm.predicate/expression (= q2.partno q3.partno)
                   :qgm.predicate/quantifiers #{q2 q3}}]
                (map #(s/assert :qgm/node %))
                (transact {}))]

    (entity db 'b3)
    (datoms db :aev :qgm.box.body/quantifiers 'b3)))
