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
        tx (with-open [api (Crux/startNode opts)]
             (api/submit-tx api [[:crux.tx/put {:crux.db/id :foo}]])
             (Thread/sleep 10) ; to avoid two txs at the same ms
             (api/submit-tx api [[:crux.tx/put {:crux.db/id :foo}]]))]

    (with-open [api2 (Crux/startNode opts)]
      (api/await-tx api2 tx nil)
      (t/is (= 2 (count (api/entity-history (api/db api2) :foo :asc)))))))
