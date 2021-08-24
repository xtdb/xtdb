(ns crux.http-server-test
  (:require [crux.fixtures :as fix :refer [*api*]]
            [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures.http-server :as fh]))

(t/use-fixtures :each
  (fix/with-opts {:crux.http-server/server {:read-only? true}})
  fh/with-http-server fix/with-node fh/with-http-client)

(t/deftest test-read-only-node
  (t/is (thrown-with-msg? UnsupportedOperationException #"read-only"
                          (crux/submit-tx *api* [[:crux.tx/put {:xt/id :foo}]]))))
