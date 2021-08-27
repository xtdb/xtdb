(ns xtdb.jdbc.tx-time-tz-test
  (:require [clojure.test :as t]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.jdbc :as fj]
            [xtdb.api :as xt]
            [xtdb.tx :as tx])
  (:import (java.util TimeZone)))

(t/use-fixtures :each fj/with-each-jdbc-dialect fj/with-jdbc-node)

(defmacro with-tz [tz-str & body]
  `(let [tz-str# ~tz-str
         orig-tz# (TimeZone/getDefault)]
     (try
       (when tz-str#
         (TimeZone/setDefault (TimeZone/getTimeZone ^String tz-str#)))
       ~@body
       (finally
         (TimeZone/setDefault orig-tz#)))))

(t/deftest test-tx-time-tz-1071
  (let [tx-time (fix/with-node
                  (fn []
                    (let [tx (xt/submit-tx *api* [[:xt/put {:xt/id :foo}]])]
                      (xt/sync *api*)
                      (t/is (= (:xt/tx-time tx)
                               (:xt/tx-time (xt/latest-completed-tx *api*))))
                      (:xt/tx-time tx))))]
    (doseq [tz-str ["Etc/UTC" "Japan/Tokyo" "Europe/London"]]
      (t/testing (str "TZ: " tz-str)
        (with-tz tz-str
          (fix/with-node
            (fn []
              (xt/sync *api*)
              (t/is (= tx-time (:xt/tx-time (xt/latest-completed-tx *api*)))))))))))
