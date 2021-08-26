(ns crux.logging-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as log-impl]
            [crux.api :as xt]
            [crux.fixtures :as fix :refer [*api*]]))

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

(t/use-fixtures :once fix/with-node)
(t/use-fixtures :each asserting-no-logged-secrets)

(t/deftest test-submit-putting-doc
  (fix/submit+await-tx [[:xt/put {:xt/id :secure-document
                                  :secret secret}]]))

(t/deftest test-submitting-putting-existing-doc
  (fix/submit+await-tx [[:xt/put {:xt/id :secure-document
                                  :secret-2 secret}]]))

(t/deftest test-submitting-match
  (fix/submit+await-tx [[:xt/match
                         :secure-document
                         {:xt/id :secure-document
                          :secret secret
                          :secret-2 secret}]
                        [:xt/put {:xt/id :secure-document, :secret secret}]]))

(t/deftest test-submitting-delete
  (fix/submit+await-tx [[:xt/delete :secure-document]]))

(t/deftest test-submitting-evict
  (fix/submit+await-tx [[:xt/evict :secure-document]]))

(t/deftest test-query-with-args
  (xt/q (xt/db *api*) {:find ['s 'ss]
                         :where [['e :secret 's]
                                 ['e :secret-2 'ss]]
                         :args [{'ss secret}]}))

(t/deftest test-querying-doc
  (xt/q (xt/db *api*) {:find ['s] :where [['e :secret 's]]}))
