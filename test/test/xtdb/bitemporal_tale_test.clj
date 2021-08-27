(ns xtdb.bitemporal-tale-test
  (:require [xtdb.api :as xt]
            [clojure.test :as t]
            [clojure.java.io :as io]))

(t/deftest bitemporal-tale-test
  (with-open [node (xt/start-node {})]
    (t/is node)

    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/put
                           {:xt/id :ids.people/Charles
                            :person/name "Charles"
                            :person/born #inst "1700-05-18"
                            :person/location :ids.places/rarities-shop
                            :person/str  40
                            :person/int  40
                            :person/dex  40
                            :person/hp   40
                            :person/gold 10000}
                           #inst "1700-05-18"]]))

    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/put
                           {:xt/id :ids.people/Mary
                            :person/name "Mary"
                            :person/born #inst "1710-05-18"
                            :person/location :ids.places/carribean
                            :person/str  40
                            :person/int  50
                            :person/dex  50
                            :person/hp   50}
                           #inst "1710-05-18"]
                          [:xt/put
                           {:xt/id :ids.people/Joe
                            :person/name "Joe"
                            :person/born #inst "1715-05-18"
                            :person/location :ids.places/city
                            :person/str  39
                            :person/int  40
                            :person/dex  60
                            :person/hp   60
                            :person/gold 70}
                           #inst "1715-05-18"]]))
    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/put
                           {:xt/id :ids.artefacts/cozy-mug
                            :artefact/title "A Rather Cozy Mug"
                            :artefact.perks/int 3}
                           #inst "1625-05-18"]

                          [:xt/put
                           {:xt/id :ids.artefacts/forbidden-beans
                            :artefact/title "Magic beans"
                            :artefact.perks/int 30
                            :artefact.perks/hp -20}
                           #inst "1500-05-18"]

                          [:xt/put
                           {:xt/id :ids.artefacts/pirate-sword
                            :artefact/title "A used sword"}
                           #inst "1710-05-18"]

                          [:xt/put
                           {:xt/id :ids.artefacts/flintlock-pistol
                            :artefact/title "Flintlock pistol"}
                           #inst "1710-05-18"]

                          [:xt/put
                           {:xt/id :ids.artefacts/unknown-key
                            :artefact/title "Key from an unknown door"}
                           #inst "1700-05-18"]

                          [:xt/put
                           {:xt/id :ids.artefacts/laptop
                            :artefact/title "A Tell DPS Laptop (what?)"}
                           #inst "2016-05-18"]]))

    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/put
                           {:xt/id :ids.places/continent
                            :place/title "Ah The Continent"}
                           #inst "1000-01-01"]
                          [:xt/put
                           {:xt/id :ids.places/carribean
                            :place/title "Ah The Good Ol Carribean Sea"
                            :place/location :ids.places/carribean}
                           #inst "1000-01-01"]
                          [:xt/put
                           {:xt/id :ids.places/coconut-island
                            :place/title "Coconut Island"
                            :place/location :ids.places/carribean}
                           #inst "1000-01-01"]]))
    (def db (xt/db node))
    (t/is (= {:xt/id :ids.people/Charles,
              :person/str 40,
              :person/dex 40,
              :person/location :ids.places/rarities-shop,
              :person/hp 40,
              :person/int 40,
              :person/name "Charles",
              :person/gold 10000,
              :person/born #inst "1700-05-18T00:00:00.000-00:00"}
             (xt/entity db :ids.people/Charles)))

    (t/is (= #{[:ids.people/Charles]}
             (xt/q db
                     '[:find ?entity-id
                       :where
                       [?entity-id
                        :person/name
                        "Charles"]])))

    (t/is (= #{[:ids.people/Charles "Charles" 40]}
             (xt/q db
                     '[:find ?e ?name ?int
                       :where
                       [?e :person/name "Charles"]
                       [?e :person/name ?name]
                       [?e :person/int  ?int]])))
    (t/is (= #{["Key from an unknown door"] ["Magic beans"]
               ["A used sword"] ["A Rather Cozy Mug"]
               ["A Tell DPS Laptop (what?)"]
               ["Flintlock pistol"]}
             (xt/q db
                     '[:find ?name
                       :where
                       [_ :artefact/title ?name]])))
    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/delete :ids.artefacts/forbidden-beans
                           #inst "1690-05-18"]]))

    (xt/await-tx node (xt/submit-tx
                         node
                         [[:xt/evict :ids.artefacts/laptop]]))

    (t/is (= #{["Key from an unknown door"] ["A used sword"] ["A Rather Cozy Mug"] ["Flintlock pistol"]}
             (xt/q (xt/db node)
                     '[:find ?name
                       :where
                       [_ :artefact/title ?name]])))

    (def world-in-1599 (xt/db node #inst "1599-01-01"))

    (t/is world-in-1599)

    (t/is (= #{["Magic beans"]}
             (xt/q world-in-1599
                     '[:find ?name
                       :where
                       [_ :artefact/title ?name]])))


    (defn first-ownership-tx []
      [(let [charles (xt/entity (xt/db node #inst "1725-05-17") :ids.people/Charles)]
         [:xt/put
          (update charles
                  :person/has
                  (comp set conj)
                  :ids.artefacts/cozy-mug
                  :ids.artefacts/unknown-key)
          #inst "1725-05-18"])

       (let [mary  (xt/entity (xt/db node #inst "1715-05-17") :ids.people/Mary)]
         [:xt/put
          (update mary
                  :person/has
                  (comp set conj)
                  :ids.artefacts/pirate-sword
                  :ids.artefacts/flintlock-pistol)
          #inst "1715-05-18"])])

    (def first-ownership-tx-response
      (xt/submit-tx node (first-ownership-tx)))

    (xt/await-tx node first-ownership-tx-response)

    (def who-has-what-query
      '[:find ?name ?atitle
        :where
        [?p :person/name ?name]
        [?p :person/has ?artefact-id]
        [?artefact-id :artefact/title ?atitle]])

    (t/is (= #{["Mary" "A used sword"]
               ["Mary" "Flintlock pistol"]
               ["Charles" "A Rather Cozy Mug"]
               ["Charles" "Key from an unknown door"]}
             (xt/q (xt/db node #inst "1726-05-01") who-has-what-query)))

    (t/is (= #{["Mary" "A used sword"] ["Mary" "Flintlock pistol"]}
             (xt/q (xt/db node #inst "1716-05-01") who-has-what-query)))

    (def parametrized-query
      '[:find ?name
        :args {ids #{:ids.people/Charles :ids.people/Mary}}
        :where
        [?e :person/name ?name]
        [(contains? ids ?e)]
        :limit 10])

    (t/is (= #{["Mary"] ["Charles"]}
             (set (xt/q (xt/db node #inst "1726-05-01") parametrized-query))))

    (defn entity-update
      [entity-id new-attrs valid-time]
      (let [entity-prev-value (xt/entity (xt/db node) entity-id)]
        (xt/submit-tx node
                        [[:xt/put
                          (merge entity-prev-value new-attrs)
                          valid-time]])))

    (defn q
      [query]
      (xt/q (xt/db node) query))

    (defn entity
      [entity-id]
      (xt/entity (xt/db node) entity-id))

    (defn entity-at
      [entity-id valid-time]
      (xt/entity (xt/db node valid-time) entity-id))

    (defn entity-with-adjacent
      [entity-id keys-to-pull]
      (let [db (xt/db node)
            ids->entities
            (fn [ids]
              (cond-> (map #(xt/entity db %) ids)
                (set? ids) set
                (vector? ids) vec))]
        (reduce
         (fn [e adj-k]
           (let [v (get e adj-k)]
             (assoc e adj-k
                    (cond
                      (keyword? v) (xt/entity db v)
                      (or (set? v)
                          (vector? v)) (ids->entities v)
                      :else v))))
         (xt/entity db entity-id)
         keys-to-pull)))

    (xt/await-tx node (entity-update :ids.people/Charles
                                       {:person/int  50}
                                       #inst "1730-05-18"))
    (t/is (= (entity :ids.people/Charles)
             {:person/str 40,
              :person/dex 40,
              :person/has #{:ids.artefacts/cozy-mug :ids.artefacts/unknown-key}
              :person/location :ids.places/rarities-shop,
              :person/hp 40,
              :person/int 50,
              :person/name "Charles",
              :xt/id :ids.people/Charles,
              :person/gold 10000,
              :person/born #inst "1700-05-18T00:00:00.000-00:00"}))

    (t/is (= (entity-with-adjacent :ids.people/Charles [:person/has])
             {:xt/id :ids.people/Charles,
              :person/str 40,
              :person/dex 40,
              :person/has
              #{{:xt/id :ids.artefacts/unknown-key,
                 :artefact/title "Key from an unknown door"}
                {:xt/id :ids.artefacts/cozy-mug,
                 :artefact/title "A Rather Cozy Mug",
                 :artefact.perks/int 3}},
              :person/location :ids.places/rarities-shop,
              :person/hp 40,
              :person/int 50,
              :person/name "Charles",
              :person/gold 10000,
              :person/born #inst "1700-05-18T00:00:00.000-00:00"}))

    (xt/await-tx node (let [theft-date #inst "1740-06-18"]
                          (xt/submit-tx
                           node
                           [[:xt/put
                             (update (entity-at :ids.people/Charles theft-date)
                                     :person/has
                                     disj
                                     :ids.artefacts/cozy-mug)
                             theft-date]
                            [:xt/put
                             (update (entity-at :ids.people/Mary theft-date)
                                     :person/has
                                     (comp set conj)
                                     :ids.artefacts/cozy-mug)
                             theft-date]])))
    (t/is (= #{["Mary" "A used sword"]
               ["Mary" "Flintlock pistol"]
               ["Mary" "A Rather Cozy Mug"]
               ["Charles" "Key from an unknown door"]}
             (xt/q (xt/db node #inst "1740-06-18") who-has-what-query)))

    (xt/await-tx node (let [marys-birth-inst #inst "1710-05-18"
                              db (xt/db node marys-birth-inst)
                              baby-mary (xt/entity db :ids.people/Mary)]
                          (xt/submit-tx
                           node
                           [[:xt/match
                             :ids.people/Mary
                             baby-mary
                             marys-birth-inst]
                            [:xt/put
                             (update baby-mary :person/has (comp set conj) :ids.artefacts/cozy-mug)
                             marys-birth-inst]])))


    (xt/await-tx node (let [mug-lost-date  #inst "1723-01-09"
                              db (xt/db node mug-lost-date)
                              mary (xt/entity db :ids.people/Mary)]
                          (xt/submit-tx
                           node
                           [[:xt/match
                             :ids.people/Mary
                             mary
                             mug-lost-date]
                            [:xt/put
                             (update mary :person/has (comp set disj) :ids.artefacts/cozy-mug)
                             mug-lost-date]])))
    (t/is (= #{["Mary" "A used sword"] ["Mary" "Flintlock pistol"]}
             (xt/q
              (xt/db node #inst "1715-05-18")
              who-has-what-query)))

    (xt/await-tx node (xt/submit-tx node (first-ownership-tx)))

    (t/is (= #{["Mary" "A used sword"]
               ["Mary" "Flintlock pistol"]
               ["Mary" "A Rather Cozy Mug"]}
             (xt/q
              (xt/db node #inst "1715-05-18")
              who-has-what-query)))

    (t/is (= #{["Mary" "A used sword"]
               ["Mary" "Flintlock pistol"]
               ["Mary" "A Rather Cozy Mug"]
               ["Charles" "Key from an unknown door"]}
             (xt/q
              (xt/db node #inst "1740-06-19")
              who-has-what-query)))

    (t/is (= #{["Mary" "A used sword"]
               ["Mary" "Flintlock pistol"]}
             (xt/q
              (xt/db node
                       #inst "1715-06-19"
                       (::xt/tx-time first-ownership-tx-response))
              who-has-what-query)))))
