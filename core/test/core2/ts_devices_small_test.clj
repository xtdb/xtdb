(ns core2.ts-devices-small-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.metadata :as meta]
            [core2.test-util :as tu]
            [core2.ts-devices :as tsd]
            [core2.util :as util]
            [clojure.tools.logging :as log]
            [core2.snapshot :as snap]
            [core2.operator :as op])
  (:import core2.metadata.IMetadataManager
           java.time.Duration))

(def ^:private ^:dynamic *node*)

(t/use-fixtures :once
  (fn [f]
    (let [node-dir (util/->path "target/can-ingest-ts-devices-small")]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (binding [*node* node]
          (t/is (nil? (c2/latest-completed-tx node)))

          (let [^IMetadataManager mm (::meta/metadata-manager @(:!system node))
                last-tx-instant (tsd/submit-ts-devices node {:size :small})]

            (log/info "transactions submitted, last tx" (pr-str last-tx-instant))
            (t/is (= last-tx-instant (tu/then-await-tx last-tx-instant node (Duration/ofMinutes 15))))
            (t/is (= last-tx-instant (c2/latest-completed-tx node)))
            (tu/finish-chunk node)

            (t/is (= [last-tx-instant (dec 1001000)]
                     @(meta/with-latest-metadata mm
                        (juxt meta/latest-tx meta/latest-row-id)))))

          (f))))))

(t/deftest ^:timescale test-recent-battery-temperatures
  (let [db (snap/snapshot (tu/component ::snap/snapshot-factory))]
    (t/is (= [{:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000000",
               :battery-temperature 91.9}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000001",
               :battery-temperature 92.6}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000002",
               :battery-temperature 87.2}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000003",
               :battery-temperature 90.5}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000004",
               :battery-temperature 88.9}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000005",
               :battery-temperature 87.4}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000006",
               :battery-temperature 88.9}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000007",
               :battery-temperature 87.4}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000008",
               :battery-temperature 91.1}
              {:time #inst "2016-11-15T18:39:00.000-00:00",
               :device-id "demo000009",
               :battery-temperature 91.1}]
             (into [] (op/plan-ra tsd/query-recent-battery-temperatures db))))))

(t/deftest ^:timescale test-busiest-low-battery-devices
  (let [db (snap/snapshot (tu/component ::snap/snapshot-factory))]
    #_ ; TODO will fill these in once we've resolved issues in ts-devices ingest
    (t/is (= []
             (into [] (op/plan-ra tsd/query-busiest-low-battery-devices db))))))

(t/deftest ^:timescale test-min-max-battery-levels-per-hour
  (let [db (snap/snapshot (tu/component ::snap/snapshot-factory))]
    #_ ; TODO will fill these in once we've resolved issues in ts-devices ingest
    (t/is (= []
             (into [] (op/plan-ra tsd/query-min-max-battery-levels-per-hour db))))))
