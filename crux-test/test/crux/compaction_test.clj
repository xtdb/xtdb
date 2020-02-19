(ns crux.compaction-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as f]
            [crux.io :as cio]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io])
  (:import crux.api.Crux
           [java.nio.file Files]))

(def ^:dynamic *db-name*)

(defn- with-prep-for-tests [f]
  (f/with-tmp-dir "compaction-test" [dir]
    (let [db-name (.getPath (io/file dir "cruxtest"))
          ds (jdbc/get-datasource {:dbtype "h2", :dbname db-name})]
      (j/prep-for-tests! "h2" ds)
      (binding [*db-name* db-name]
        (f)))))

(t/use-fixtures :each with-prep-for-tests)

(t/deftest test-compaction-leaves-replayable-log
  (let [opts {:crux.node/topology 'crux.jdbc/topology
              :crux.jdbc/dbtype "h2"
              :crux.jdbc/dbname *db-name*}
        api (Crux/startNode opts)
        tx (api/submit-tx api [[:crux.tx/put {:crux.db/id :foo}]])]
    (api/await-tx api tx)
    (f/transact! api [{:crux.db/id :foo}])
    (.close api)

    (with-open [api2 (Crux/startNode opts)]
      (api/await-tx api2 tx nil)
      (t/is (= 2 (count (api/history api2 :foo)))))))
