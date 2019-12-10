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
  (let [ret (f)]
    (io/delete-file "logtester.log")
    ret))

(t/use-fixtures :once kvf/with-kv-dir fs/with-standalone-node apif/with-node)

(defn- sync-submit-tx [node tx-ops]
  (let [submitted-tx (api/submit-tx node tx-ops)]
    (api/sync node (:crux.tx/tx-time submitted-tx) nil)
    submitted-tx))

(t/deftest test-submit-tx-log
  (let [secret 33489857205]
    (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                         :secret secret}]])
    (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                          :secret-2 secret}]])
    (t/is (not (re-find (re-pattern secret) (slurp "logtester.log"))))))

