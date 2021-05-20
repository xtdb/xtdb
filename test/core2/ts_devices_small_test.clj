(ns core2.ts-devices-small-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.metadata :as meta]
            [core2.test-util :as tu]
            [core2.ts-devices :as tsd]
            [core2.util :as util])
  (:import core2.metadata.IMetadataManager
           java.time.Duration))

(t/deftest ^:timescale can-ingest-ts-devices-small
  (let [node-dir (util/->path "target/can-ingest-ts-devices-small")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (nil? (c2/latest-completed-tx node)))

      (let [^IMetadataManager mm (:core2/metadata-manager @(:!system node))
            last-tx-instant (tsd/submit-ts-devices node :small {:batch-size 100})]

        (t/is (= last-tx-instant (c2/await-tx node last-tx-instant (Duration/ofMinutes 15))))
        (t/is (= last-tx-instant (c2/latest-completed-tx node)))
        (tu/finish-chunk node)

        (t/is (= [last-tx-instant (dec 1001000)]
                 @(meta/with-latest-metadata mm
                    (juxt meta/latest-tx meta/latest-row-id))))))))
