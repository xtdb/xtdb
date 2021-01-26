;; TODO kill this namespace
(ns crux.examples-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix]
            [crux.docs.examples :as ex]))

(def ^:dynamic *storage-dir*)

(defn with-storage-dir [f]
  (fix/with-tmp-dir "storage" [storage-dir]
    (binding [*storage-dir* storage-dir]
      (f))))

(t/use-fixtures :each with-storage-dir)

(t/deftest basic-test-examples
  (doseq [tg ex/basic-tests]
    (t/testing (:n tg)
      (with-open [node (crux/start-node {})]
        (crux/await-tx node ((:s tg) node) nil)
        (doseq [t (:t tg)]
          (t/testing (:d t)
            (t/is (= (:e t) ((:q t) node)))))))))

(t/deftest test-streaming-queries
  (with-open [node (crux/start-node {})]
    (crux/await-tx node (ex/query-example-setup node) nil)
    (let [!results (atom [])]
      (ex/query-example-streaming node #(swap! !results conj %))
      (t/is (= [[:smith]] @!results)))))
