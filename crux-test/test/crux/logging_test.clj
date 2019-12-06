#_((ns crux.logging-test
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

(t/use-fixtures :each kvf/with-kv-dir fs/with-standalone-node apif/with-node)

(t/deftest test-submit-tx-log
  (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :secure-document
                                       :secret 33489857205}]]))

)
