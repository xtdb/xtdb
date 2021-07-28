(ns crux.jdbc.tx-time-tz-test
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.jdbc :as fj]
            [crux.api :as crux]
            [crux.tx :as tx])
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
                    (let [tx (crux/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
                      (crux/sync *api*)
                      (t/is (= (::tx/tx-time tx)
                               (::tx/tx-time (crux/latest-completed-tx *api*))))
                      (::tx/tx-time tx))))]
    (doseq [tz-str ["Etc/UTC" "Japan/Tokyo" "Europe/London"]]
      (t/testing (str "TZ: " tz-str)
        (with-tz tz-str
          (fix/with-node
            (fn []
              (crux/sync *api*)
              (t/is (= tx-time (::tx/tx-time (crux/latest-completed-tx *api*)))))))))))
