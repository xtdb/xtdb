(ns patrik)

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
           (fn [acc [window-name {:keys [partition-by order-by reductions]}]]
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
                       (fn [new-state [field {:keys [aggregator]}]]
                         (let [p-value (or (get prev-state field)
                                           (aggregator))]
                           (assoc new-state field
                                  (aggregator p-value input ;; gather the input to the aggrigator
                                              ))))
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
      (throw (ex-info "unexpected value not in collection" {:value value
                                                            :coll coll}))
      (if (= h value)
        i
        (recur (inc i) t)))))

(defn aggr-count
  ([] 0)
  ([i] i)
  ([acc _i] (inc acc)))

(defn aggregate-decorate-datalog
  [{:as datalog
    {:keys [partition-by project]} :aggrigation}]
  (let [find-variables (->> (concat (map second partition-by)
                                    (mapcat
                                      (fn [[key f inputs]] (vals inputs))
                                      project))
                            set vec)]
    {:aggregation-spec
     {:windows
      {:aggr/output
       {:partition-by (for [[k v] partition-by]
                        (let [idx (index-of v find-variables)]
                          [k (fn [val] (get val idx))]))
        :reductions (into {} (for [[k f inputs] project]
                               [k {:aggregator (resolve f)}]))}}}

     :datalog (merge datalog {:find find-variables})}))

(defn run-datalog-aggregation
  [db {:keys [aggregation-spec datalog]}]
  (transduce
    (aggregation-runner aggregation-spec)
    conj
    {}
    (crux.api/q db datalog)))

(comment

  (clojure.pprint/pprint
    (aggregate-decorate-datalog
      '{:partition-by [[:name ?name]]
        :project [[:post-count patrik/aggr-count {:post-id ?post-id}]]

        :where [[?post :user/name ?name]
                [?post :user/post ?post-id]]}))


  (require 'crux.api)
  (def crux (crux.api/start-standalone-system {:kv-backend "crux.kv.rocksdb.RocksKv"
                                               :db-dir "data/db-dir"
                                               :event-log-dir "data/event-log-db"}))

  (crux.api/submit-tx crux [[:crux.tx/put :a1
                             {:crux.db/id :a1 :user/name "patrik" :user/post 1}]
                            [:crux.tx/put :a2
                             {:crux.db/id :a2 :user/name "patrik" :user/post 2}]
                            [:crux.tx/put :a3
                             {:crux.db/id :a3 :user/name "patrik" :user/post 3}]
                            [:crux.tx/put :a4
                             {:crux.db/id :a4 :user/name "niclas" :user/post 3}]])


  (clojure.pprint/pprint
    (run-datalog-aggregation
      (crux.api/db crux)
      (aggregate-decorate-datalog
        '{:aggrigation {:partition-by [[:name ?name]]
                        :project [[:post-count patrik/aggr-count {:post-id ?post-id}]]}

          :where [[?post :user/name ?name]
                  [?post :user/post ?post-id]]})))

  (transduce
    (aggregation-runner
      {:windows
       {:aggr/output
        {:partition-by [[:name :name]]
         :reductions {:user-count {:aggregator count}}}}})
    conj
    {}
    [{:name "patrik" :post "1"}
     {:name "patrik" :post "2"}
     {:name "patrik" :post "3"}

     {:name "niclas" :post "1"}])


  )
