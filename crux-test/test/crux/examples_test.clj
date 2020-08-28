(ns crux.examples-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix]
            [docs.examples :as ex]))

(def ^:dynamic *storage-dir*)

(defn with-storage-dir [f]
  (fix/with-tmp-dir "storage" [storage-dir]
    (binding [*storage-dir* storage-dir]
      (f))))

(t/use-fixtures :each with-storage-dir)

(t/deftest test-example-basic-queries
  (with-open [node (crux/start-node {})]
    (crux/await-tx node (ex/query-example-setup node) nil)
    (t/is (= #{[:smith]} (ex/query-example-basic-query node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-1 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-2 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-3 node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-4 node)))
    (t/is (= #{[22]} (ex/query-example-with-arguments-5 node)))
    (t/is (= #{[21]} (ex/query-example-with-predicate-1 node)))

    (let [!results (atom [])]
      (ex/query-example-streaming node #(swap! !results conj %))
      (t/is (= [[:smith]] @!results)))))

(t/deftest test-example-time-queries
  (with-open [node (crux/start-node {})]
    (crux/await-tx node (ex/query-example-at-time-setup node) nil)
    (t/is (= #{} (ex/query-example-at-time-q1 node)))
    (t/is (= #{[:malcolm]} (ex/query-example-at-time-q2 node)))))

(t/deftest test-example-join-queries
  (with-open [node (crux/start-node {})]
    (crux/await-tx node (ex/query-example-join-q1-setup node) nil)

    (t/is (= #{[:ivan :ivan]
               [:petr :petr]
               [:sergei :sergei]
               [:denis-a :denis-a]
               [:denis-b :denis-b]
               [:denis-a :denis-b]
               [:denis-b :denis-a]}
             (ex/query-example-join-q1 node)))

    (crux/await-tx node (ex/query-example-join-q2-setup node) nil)
    (t/is (= #{[:petr]}
             (ex/query-example-join-q2 node)))))
