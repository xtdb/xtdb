(ns xtdb.disk-join-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.operator.join :as join]
            [xtdb.test-util :as tu]))

(defn with-disk-threshold
  ([threshold] (partial with-disk-threshold threshold))
  ([threshold f]
   (binding [join/*disk-join-threshold* threshold]
     (f))))

(t/use-fixtures :each tu/with-mock-clock tu/with-node (with-disk-threshold 1000))

(deftest ^:integration test-on-disk-joining
  (let [ids (range 100000)]
    (doseq [batch (partition-all 1000 ids)
            :let [docs (for [id batch]
                         {:xt/id id})]]
      (xt/submit-tx tu/*node*
                    [(into [:put-docs :foo] docs)
                     (into [:put-docs :bar] docs)]))
    (t/is (=  [{:cnt 100000}]
              (xt/q tu/*node* "SELECT COUNT(DISTINCT foo._id) AS cnt
                               FROM foo, bar
                               WHERE foo._id = bar._id")))))
