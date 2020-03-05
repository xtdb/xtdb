(ns crux.sorted-maps-microbench)

(comment
  (let [submitted-tx (last (for [doc-batch (->> (for [n (range 25000)]
                                                  [:crux.tx/put {:crux.db/id (keyword (str "doc-" n))
                                                                 :doc-idx n
                                                                 :nested-map {:foo :bar
                                                                              :baz :quux}}])
                                                (partition-all 5000))]
                             (time (crux/submit-tx (user/crux-node) doc-batch))))]

    (time (crux/await-tx (user/crux-node) submitted-tx (java.time.Duration/ofSeconds 20)))))
