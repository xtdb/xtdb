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

(def secret 33489857205)

(defn asserting-no-logged-secrets [f]
  (let [!log-messages (atom [])]
    (with-redefs [log-impl/enabled? (constantly true)

                  log/log* (fn [logger level throwable message]
                             (swap! !log-messages conj message)
                             nil)]
      (f)

      (t/is (every? (complement #(re-find (re-pattern (str secret)) %))
                    @!log-messages)))))

(t/use-fixtures :once kvf/with-kv-dir fs/with-standalone-node fapi/with-node)
(t/use-fixtures :each asserting-no-logged-secrets)

(t/deftest test-submit-putting-doc
  (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :secure-document
                                        :secret secret}]]))

(t/deftest test-submitting-putting-existing-doc
  (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :secure-document
                                        :secret-2 secret}]]))

(t/deftest test-submitting-match
  (fapi/submit+await-tx [[:crux.tx/match
                          :secure-document
                          {:crux.db/id :secure-document
                           :secret secret
                           :secret-2 secret}]
                         [:crux.tx/put {:crux.db/id :secure-document, :secret secret}]]))

(t/deftest test-submitting-delete
  (fapi/submit+await-tx [[:crux.tx/delete :secure-document]]))

(t/deftest test-submitting-evict
  (fapi/submit+await-tx [[:crux.tx/evict :secure-document]]))

(t/deftest test-query-with-args
  (api/q (api/db *api*) {:find ['s 'ss]
                         :where [['e :secret 's]
                                 ['e :secret-2 'ss]]
                         :args [{'ss secret}]}))

(t/deftest test-querying-doc
  (api/q (api/db *api*) {:find ['s] :where [['e :secret 's]]}))
