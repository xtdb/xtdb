(ns xtdb.fixtures.document-store
  (:require [clojure.test :as t]
            [xtdb.codec :as c]
            [xtdb.db :as db]))

(defn test-doc-store [doc-store]
  (let [alice {:xt/id :alice, :name "Alice"}
        alice-key (c/new-id alice)
        bob {:xt/id :bob, :name "Bob"}
        bob-key (c/new-id bob)
        max-key (c/new-id {:xt/id :max, :name "Max"})
        people {alice-key alice, bob-key bob}]

    (db/submit-docs doc-store people)

    (t/is (= {alice-key alice}
             (db/fetch-docs doc-store #{alice-key})))

    (t/is (= people
             (db/fetch-docs doc-store (conj (keys people) max-key))))

    (let [evicted-alice {:xt/id :alice, :xt/evicted? true}]
      (db/submit-docs doc-store {alice-key evicted-alice})

      (t/is (= {alice-key evicted-alice, bob-key bob}
               (db/fetch-docs doc-store (keys people)))))))
