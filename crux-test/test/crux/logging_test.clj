(ns crux.logging-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as log-impl]
            [crux.api :as api]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

(def !log-messages (atom []))
(defn with-log-redef [f]
  (with-redefs [log-impl/enabled? (constantly true)
                log/log* (fn [logger level throwable message]
                           (println (count @!log-messages))
                           (swap! !log-messages conj message))]
    (f)))

(t/use-fixtures :once kvf/with-kv-dir fs/with-standalone-node fapi/with-node with-log-redef)

(def secret 33489857205)
(defn check-and-reset! []
  (let [res (every? #(not (re-find (re-pattern (str secret)) %)) @!log-messages)]
    (reset! !log-messages [])
    res))

(t/deftest test-submit-tx-log
  (t/testing "Put"
    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :secure-document
                                          :secret secret}]])
    (t/is (check-and-reset!)))

  (t/testing "Query on doc"
    (api/q (api/db *api*) {:find ['s] :where [['e :secret 's]]})
    (t/is (check-and-reset!)))

  (t/testing "Put on existing doc"
    (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :secure-document
                                          :secret-2 secret}]])
    (t/is (check-and-reset!)))

  (t/testing "Query on doc with args" (api/q (api/db *api*) {:find ['s 'ss]
                                                             :where [['e :secret 's]
                                                                     ['e :secret-2 'ss]]
                                                             :args [{'ss secret}]})
    (t/is (check-and-reset!)))

  (t/testing "CAS"
    (fapi/submit+await-tx [[:crux.tx/cas
                            {:crux.db/id :secure-document
                             :secret secret
                             :secret-2 secret}
                            {:crux.db/id :secure-document
                             :secret secret}]])
    (t/is (check-and-reset!)))

  (t/testing "Delete"
    (fapi/submit+await-tx [[:crux.tx/delete :secure-document]])
    (t/is (check-and-reset!)))

  (t/testing "Evict" (sync-submit-tx *api* [[:crux.tx/evict :secure-document]])
    (t/is (check-and-reset!))))
