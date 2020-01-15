(ns crux.logging-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.fixtures :as f]
            [crux.fixtures.api :as apif :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

(defn remove-log-file [f]
  (do
    (f)
    (io/delete-file "logtester.log")))

(t/use-fixtures :once kvf/with-kv-dir fs/with-standalone-node apif/with-node remove-log-file)

(defn- sync-submit-tx [node tx-ops]
  (let [submitted-tx (api/submit-tx node tx-ops)]
    (api/sync node (:crux.tx/tx-time submitted-tx) nil)
    submitted-tx))

(t/deftest test-submit-tx-log
  (let [secret 33489857205]

    (t/testing "Put"
      (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                            :secret secret}]])
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "Query on doc"
      (api/q (api/db *api*) {:find ['s] :where [['e :secret 's]]})
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "Put on existing doc"
      (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                            :secret-2 secret}]])
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "Query on doc with args" (api/q (api/db *api*) {:find ['s 'ss]
                                                               :where [['e :secret 's]
                                                                       ['e :secret-2 'ss]]
                                                               :args [{'ss secret}]})
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "CAS"
      (sync-submit-tx *api* [[:crux.tx/cas
                              {:crux.db/id :secure-document
                               :secret secret
                               :secret-2 secret}
                              {:crux.db/id :secure-document
                               :secret secret}]])
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "Delete"
      (sync-submit-tx *api* [[:crux.tx/delete :secure-document]])
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/testing "Evict" (sync-submit-tx *api* [[:crux.tx/evict :secure-document]])
      (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log")))))

    (t/is (slurp "logtester.log"))))

