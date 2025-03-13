(ns xtdb.migration-test)

;; to regenerate the test files, run this against 2.0.0-beta6
(comment
  (require '[xtdb.api :as xt]
           '[xtdb.compactor :as c]
           '[xtdb.test-util :as tu]
           '[xtdb.time :as time]
           '[xtdb.util :as util])

  (let [node-dir (util/->path "src/test/resources/xtdb/migration-test/v05-v06/")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              c/*ignore-signal-block?* true
              c/*l1-file-size-rows* 250]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 120})]
        (doseq [tick (range 25)
                batch (->> (range 100)
                           (partition-all 64))
                :let [tick-at (-> (time/->zdt #inst "2020-01-01") (.plusHours (* tick 12)))]]
          (xt/execute-tx node [(into [:put-docs {:into :readings, :valid-from tick-at, :valid-to (.plusHours tick-at 12)}]
                                     (for [x batch]
                                       {:xt/id x, :reading tick}))

                               (into [:put-docs {:into :prices, :valid-from tick-at}]
                                     (for [x batch]
                                       {:xt/id x, :price tick}))]

                         {:system-time tick-at}))

        (c/compact-all! node #xt/duration "PT1S")))))
