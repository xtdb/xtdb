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
        opts {:crux.bootstrap/node-config :crux.jdbc/node-config
              :dbtype "h2"
              :dbname "cruxtest"
              :crux.kv/db-dir db-dir
              :crux.kv/kv-backend "crux.kv.memdb.MemKv"}]
    (try
      (let [api (Crux/startNode opts)
            {:keys [crux.tx/tx-time]} (api/submit-tx api [[:crux.tx/put {:crux.db/id :foo}]])]
        (api/sync api tx-time nil)
        (f/transact! api [{:crux.db/id :foo}])
        (.close api)

        (with-open [api2 (Crux/startNode opts)]
          (api/sync api2 tx-time nil)
          (t/is (= 2 (count (api/history api2 :foo))))))
      (finally
        (cio/delete-dir db-dir)))))
