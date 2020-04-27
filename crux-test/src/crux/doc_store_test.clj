(ns crux.doc-store-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.codec :as c]))

(def ^:dynamic *doc-store*)

(t/deftest round-trips-docs
  (let [alice {:crux.db/id :alice, :name "Alice"}
        alice-key (c/new-id alice)
        bob {:crux.db/id :bob, :name "Bob"}
        bob-key (c/new-id bob)
        max-key (c/new-id {:crux.db/id :max, :name "Max"})
        people {alice-key alice, bob-key bob}]

    (db/submit-docs *doc-store* people)

    (t/is (= {alice-key alice}
             (db/fetch-docs *doc-store* #{alice-key})))

    (t/is (= people
             (db/fetch-docs *doc-store* (conj (keys people) max-key))))

    (let [evicted-alice {:crux.db/id :alice, :crux.db/evicted? true}]
      (db/submit-docs *doc-store* {alice-key evicted-alice})

      (t/is (= {alice-key evicted-alice, bob-key bob}
               (db/fetch-docs *doc-store* (keys people)))))))

(defn test-doc-store [ns]
  (let [once-fixture-fn (t/join-fixtures (::t/once-fixtures (meta ns)))
        each-fixture-fn (t/join-fixtures (::t/each-fixtures (meta ns)))]
    (once-fixture-fn
     (fn []
       (doseq [v (vals (ns-interns 'crux.doc-store-test))]
         (when (:test (meta v))
           (each-fixture-fn (fn []
                              (t/test-var v)))))))))

(defn test-ns-hook []
  ;; no-op, these tests are called from elsewhere
  )
