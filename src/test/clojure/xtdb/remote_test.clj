(ns xtdb.remote-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu :refer [*node*]]))

;; ONLY put stuff here where remote DIFFERS to in-memory

(t/use-fixtures :each tu/with-mock-clock tu/with-http-client-node)

(deftest normalisation-option
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Illegal argument: "
                          (xt/q *node* '(from :docs [first-name last-name])
                                {:key-fn identity}))
        "remote can not serialize arbitrary fns"))
