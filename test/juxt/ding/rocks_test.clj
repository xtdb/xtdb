(ns juxt.rocks-test
  (:require [clojure.test :as t]
            [juxt.rocks]))

(def ^:dynamic *rocks-db*)

(defn- start-system [f]
  (binding [*rocks-db* (juxt.rocks/open-db)]
    (try
      (f)
      (finally
        (.close *rocks-db*)))))

(t/use-fixtures :each start-system)

(t/deftest test-can-get-at-now
  (juxt.rocks/-put *rocks-db* "Foo" "Bar4")
  (juxt.rocks/-put *rocks-db* "Foo" "Bar5")
  (t/is (= "Bar5" (juxt.rocks/-get-at *rocks-db* "Foo"))))
