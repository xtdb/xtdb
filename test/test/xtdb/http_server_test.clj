(ns xtdb.http-server-test
  (:require [xtdb.fixtures :as fix :refer [*api*]]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures.http-server :as fh]))

(t/use-fixtures :each
  (fix/with-opts {:xtdb.http-server/server {:read-only? true}})
  fh/with-http-server fix/with-node fh/with-http-client)

(t/deftest test-read-only-node
  (t/is (thrown-with-msg? UnsupportedOperationException #"read-only"
                          (xt/submit-tx *api* [[::xt/put {:xt/id :foo}]]))))
