(ns crux.compaction-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as f]
            [crux.io :as cio]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc])
  (:import crux.api.Crux))

(defn- with-tear-down [f]
  (let [ds (jdbc/get-datasource {:dbtype "h2"
                                 :dbname "cruxtest"})]
    (j/prep-for-tests! "h2" ds))
  (f))

(t/use-fixtures :each with-tear-down)

(t/deftest test-compaction-leaves-replayable-log
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        opts {:dbtype "h2"
              :dbname "cruxtest"
              :db-dir db-dir
              :kv-backend "crux.kv.memdb.MemKv"}]
    (try
      (let [api (Crux/startJDBCNode opts)
            {:keys [crux.tx/tx-time]} (api/submit-tx api [[:crux.tx/put {:crux.db/id :foo}]])]
        (api/sync api tx-time nil)
        (f/transact! api [{:crux.db/id :foo}])
        (.close api)

        (with-open [api2 (Crux/startJDBCNode opts)]
          (api/sync api2 tx-time nil)
          (t/is (= 2 (count (api/history api2 :foo))))))
      (finally
        (cio/delete-dir db-dir)))))
