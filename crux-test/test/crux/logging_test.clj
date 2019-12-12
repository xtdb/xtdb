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
    (io/delete-file "logtester.log")
    (f)))

(t/use-fixtures :once kvf/with-kv-dir fs/with-standalone-node apif/with-node)

(defn- sync-submit-tx [node tx-ops]
  (let [submitted-tx (api/submit-tx node tx-ops)]
    (api/sync node (:crux.tx/tx-time submitted-tx) nil)
    submitted-tx))

(t/deftest test-submit-tx-log
  (let [secret 33489857205]

    ;; Put
    (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                          :secret secret}]])
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; Query on document
    (api/q (api/db *api*) {:find ['s] :where [['e :secret 's]]})
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; Put over exisiting doc
    (sync-submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                          :secret-2 secret}]])
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; Query on doc with args
    (api/q (api/db *api*) {:find ['s 'ss]
                           :where [['e :secret 's]
                                   ['e :secret-2 'ss]]
                           :args [{'ss secret}]})
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; CAS
    (sync-submit-tx *api* [[:crux.tx/cas
                            {:crux.db/id :secure-document
                             :secret secret
                             :secret-2 secret}
                            {:crux.db/id :secure-document
                             :secret secret}]])
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; Delete
    (sync-submit-tx *api* [[:crux.tx/delete :secure-document]])
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    ;; Evict
    (sync-submit-tx *api* [[:crux.tx/evict :secure-document]])
    (t/is (not (re-find (re-pattern (str secret)) (slurp "logtester.log"))))
    (t/is (slurp "logtester.log"))))

