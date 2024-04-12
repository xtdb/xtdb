(ns multi-node.core-test
  (:require [clojure.test :refer [deftest is]]
            [multi-node.core :as core]))

(deftest this-test
  (is (= 1 2))
  (is (= 1 1)))
