(ns patrik
  (:require [clojure.pprint]
            [crux.api]))

(defn aggregation-runner
  [spec]
  (fn transducer [rx]
    (let []
      (fn
        ([] {})
        ([acc]
         (if-let [reductions (-> spec :windows :aggr/output :reductions)]
           (map
             (fn [[partition row]]
               (reduce
                 (fn [row [field {:keys [aggregator]}]]
                   (update row field aggregator))
                 row
                 reductions))
             (-> acc :aggr/output))
           (-> acc :aggr/output vals)))
        ([acc input]
         (reduce
           (fn [acc [window-name {:keys [partition-by order-by reductions] :as all}]]
             (let [partition (when partition-by
                               (reduce
                                 (fn [partition [partition-part-key partition-fn]]
                                   (assoc partition partition-part-key (partition-fn input)))
                                 {}
                                 partition-by))
                   acc
                   (if (contains? acc window-name)
                     acc
                     (let [partition->comparable (apply juxt (map first partition-by))]
                       (assoc acc window-name (sorted-map-by
                                                (fn [a b]
                                                  (compare
                                                    (partition->comparable a)
                                                    (partition->comparable b)))))))]
               (if reductions
                 (let [prev-state (get-in acc [window-name partition])]
                   (assoc-in
                     acc
                     [window-name partition]
                     (reduce
                       (fn [new-state [field {:keys [aggregator inputs]}]]
                         (let [p-value (or (get prev-state field)
                                           (aggregator))]
                           (assoc new-state field
                                  (aggregator p-value
                                              (merge partition
                                                     (into {}
                                                           (for [[k f] inputs]
                                                             [k (f input)])))))))
                       partition
                       reductions)))
                 (let [acc (if (contains? (get acc window-name) partition)
                             acc
                             (assoc-in acc [window-name partition] (sorted-set-by
                                                                     (fn [a b]
                                                                       (let [r (compare
                                                                                 (order-by a)
                                                                                 (order-by b))]
                                                                         (if (= 0 r)
                                                                           (compare (hash a) (hash b))
                                                                           r))))))]
                   (update-in
                     acc
                     [window-name partition] conj input)))))
           acc
           (:windows spec)))))))

(defn index-of
  [value coll]
  (loop [i 0
         [h & t] coll]
    (if (nil? h)
      (throw (ex-info "unexpected value not in collection"
                      {:value value :coll coll}))
      (if (= h value)
        i
        (recur (inc i) t)))))

(defn aggr-count
  ([] 0)
  ([i] i)
  ([acc _i] (inc acc)))

(defn aggregate-decorate-datalog
  [{:as datalog
    {:keys [partition-by order-by project windows]} :aggrigation}]
  (let [find-variables
        (->> (concat (map
                       (fn [item]
                         (if (vector? item) (second item) item))
                       partition-by)
                     (map second order-by)
                     (mapcat
                       (fn [[key f inputs]] (vals inputs))
                       project))
             set vec)]
    {:aggregation-spec
     {:windows
      (reduce
        (fn [acc [window-name {:keys [partition-by order-by] :as window}]]
          (let [{:keys [aggregation-spec datalog]}
                (aggregate-decorate-datalog {:aggrigation window})]
            (assoc acc window-name
                   (merge (-> aggregation-spec :windows :aggr/output)
                          {:runner-fn (fn [snapshot db] (crux.api/q db snapshot datalog))}))))
        {:aggr/output
         {:partition-by (for [item partition-by]
                          (let [k (if (vector? item) (first item) (keyword item))
                                v (if (vector? item) (second item) item)
                                idx (index-of v find-variables)]
                            [k (fn [val] (get val idx))]))
          :reductions (into {} (for [[k f inputs] project]
                                 [k {:aggregator (resolve f)
                                     :inputs (into {}
                                                   (for [[k v] inputs]
                                                     (let [idx (index-of v find-variables)]
                                                       [k (fn [val] (get val idx))])))}]))}}
        windows)}

     :datalog (merge datalog {:find find-variables})}))

(defn window->partition-components
  [{:keys [order-by partition-by]}]
  (concat
    (map first partition-by)
    (map first order-by)))

(defn partition-tree
  [aggregation-spec]
  (let [window-name->partition-components
        (map (juxt first (comp window->partition-components second))
             (:windows aggregation-spec))]
    (fn step [current-path]
      {:type :root-node
       :children
       (for [path-component (set (map first (vals window-name->partition-components)))]
         {:path path-component
          })})))

(defn run-datalog-aggregation
  [db {:keys [aggregation-spec datalog]}]
  (with-open [snapshot (crux.api/new-snapshot db)]
    (reduce
      (fn [state [window-name window-spec]]
        (transduce
          (aggregation-runner aggregation-spec)
          conj
          {}
          (crux.api/q db snapshot datalog)))
      {}
      (:windows aggregation-spec))))

(comment

  (require 'crux.api)

  (def crux (crux.api/start-node
             {:crux.node/topology :crux.standalone/topology
              :crux.node/kv-store "crux.kv.rocksdb.RocksKv"
              :crux.kv/db-dir "data/db-dir"
              :crux.standalone/event-log-dir "data/event-log-db"}))

  (crux.api/submit-tx
   crux
   [[:crux.tx/put
     {:crux.db/id :a1 :user/name "patrik" :user/post 1 :post/cost 30}]
    [:crux.tx/put
     {:crux.db/id :a2 :user/name "patrik" :user/post 2 :post/cost 35}]
    [:crux.tx/put
     {:crux.db/id :a3 :user/name "patrik" :user/post 3 :post/cost 5}]
    [:crux.tx/put
     {:crux.db/id :a4 :user/name "niclas" :user/post 1 :post/cost 8}]])

  (clojure.pprint/pprint
   (run-datalog-aggregation
    (crux.api/db crux)
    (aggregate-decorate-datalog
     '{:aggrigation {:partition-by [[:name ?name]]
                     :project [[:post-count patrik/aggr-count {:post-id ?post-id}]]}

       :where [[?post :user/name ?name]
               [?post :user/post ?post-id]]})))

  (defn avr-cost
    ([] {:count 0 :total 0})
    ([{:keys [count total]}] (/ total count))
    ([acc inputs]
     (-> acc
         (update :count inc)
         (update :total + (:cost inputs)))))

  (defn sum-cost
    ([] 0)
    ([acc] acc)
    ([acc inputs]
     (+ acc (:cost inputs))))

  (clojure.pprint/pprint
   (run-datalog-aggregation
    (crux.api/db crux)
    (aggregate-decorate-datalog
     '{:aggrigation {:windows {:user-costs {:partition-by [[:name ?name]]
                                            :order-by [[:post-id ?post-id :asc]]}}

                     :partition-by [[:name ?name]
                                    [:post-id ?post-id]]

                     :project [[:avr-cost patrik/avr-cost {:cost ?cost}]
                               [:running-cost patrik/sum-cost {:cost ?cost} :user-costs]]}

       :where [[?post :user/name ?name]
               [?post :user/post ?post-id]
               [?post :post/cost ?cost]]})))

  (clojure.pprint/pprint
   (aggregate-decorate-datalog
    '{:aggrigation {:partition-by [[:name ?name]]
                    :project [[:avr-cost patrik/avr-cost {:cost ?cost}]
                              [:running-cost patrik/sum-cost {:cost ?cost}]
                              [:post-count patrik/aggr-count {:post-id ?post-id}]]}

      :where [[?post :user/name ?name]
              [?post :user/post ?post-id]
              [?post :post/cost ?cost]]}))


  (clojure.pprint/pprint
   (run-datalog-aggregation
    (crux.api/db crux)
    (aggregate-decorate-datalog
     '{:aggrigation
       {:partition-by [?name ?post-id]
        :order-by [[?name :asc]]}

       :where [[?post :user/name ?name]
               [?post :user/post ?post-id]]})))



  (defn running-cost
    ([] 0)
    ([i] i)
    ([acc i]
     (+ acc i)))



  )
