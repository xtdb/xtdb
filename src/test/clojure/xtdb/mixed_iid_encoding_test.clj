(ns xtdb.mixed-iid-encoding-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files FileVisitOption Path StandardCopyOption)
           (org.apache.arrow.flight CallOption FlightClient FlightEndpoint Location)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FixedSizeBinaryVector)
           (org.apache.arrow.vector.types.pojo ArrowType$FixedSizeBinary ArrowType$Int ArrowType$RunEndEncoded Field FieldType Schema)
           (xtdb.api Xtdb)
           (xtdb.arrow ArrowUnloader$Mode Relation RunEndEncodedVector)))

(def ^:private ree-iid-field
  (Field. "_iid" (FieldType/notNullable ArrowType$RunEndEncoded/INSTANCE)
          [(Field. "run_ends" (FieldType/notNullable (ArrowType$Int. 32 true)) nil)
           (Field. "values" (FieldType/notNullable (ArrowType$FixedSizeBinary. 16)) nil)]))

(defn- rewrite-iid-as-runs!
  [^BufferAllocator al ^Path data-file]
  (let [tmp (.resolveSibling data-file (str (.getFileName data-file) ".ree"))
        !rows (atom 0)
        !runs (atom 0)]
    (with-open [ldr (Relation/loader al (Files/readAllBytes data-file))]
      (let [out-schema (Schema. (for [^Field f (.getFields (.getSchema ldr))]
                                  (if (= "_iid" (.getName f)) ree-iid-field f)))]
        (with-open [out (Relation. al out-schema)
                    unl (.startUnload out tmp ArrowUnloader$Mode/FILE)]
          (dotimes [page (.getPageCount ldr)]
            (with-open [src (.loadPage ldr page al)]
              (.clear out)
              (let [n (.getRowCount src)
                    src-iid (.vectorFor src "_iid")
                    ^RunEndEncodedVector out-iid (.vectorFor out "_iid")]
                (doseq [^Field f (.getFields out-schema)
                        :let [col (.getName f)]
                        :when (not= "_iid" col)]
                  (.copyRange (.rowCopier (.vectorFor src col) (.vectorFor out col)) 0 n))

                (loop [i 0]
                  (when (< i n)
                    (let [^ByteBuffer iid (.getBytes src-iid i)
                          run-end (loop [j (inc i)]
                                    (if (and (< j n) (= iid (.getBytes src-iid j))) (recur (inc j)) j))
                          bs (byte-array 16)]
                      (.get (.duplicate iid) bs)
                      (.writeRun out-iid bs (int (- run-end i)))
                      (swap! !runs inc)
                      (recur run-end))))

                (.setRowCount out n)
                (swap! !rows + n)
                (.writePage unl))))
          (.end unl))))
    (Files/move tmp data-file (into-array [StandardCopyOption/REPLACE_EXISTING]))
    {:rows @!rows, :runs @!runs}))

(defn- compacted-data-files [^Path node-dir]
  (with-open [paths (Files/walk node-dir (make-array FileVisitOption 0))]
    (->> (iterator-seq (.iterator paths))
         (filter (fn [^Path p]
                   (and (str/includes? (str p) "/data/")
                        (str/ends-with? (str p) ".arrow")
                        (not (str/starts-with? (str (.getFileName p)) "l00-")))))
         sort
         vec)))

(defn- trie-key [^Path data-file]
  (str/replace (str (.getFileName data-file)) #"\.arrow$" ""))

(def ^:private start (time/->zdt #inst "2020-01-01"))

(defn- ingest! [node ticks]
  (doseq [tick ticks
          :let [at (.plusHours start (* tick 6))]]
    (xt/execute-tx node
                   (cond-> [(into [:put-docs {:into :readings, :valid-from at}]
                                  (for [id (range 30)]
                                    {:xt/id id, :value tick}))]
                     (zero? (mod tick 10)) (conj [:delete-docs {:from :readings, :valid-from (.plusHours at 1)} 0 1 2]))
                   {:system-time (.toInstant at)}))

  (tu/flush-block! node)
  (c/compact-all! node #xt/duration "PT2M"))

(defn- iid-scan []
  '[:scan {:db-name "xtdb", :table #xt/table readings, :for-valid-time :all-time, :for-system-time :all-time
           :columns [_iid]}])

(defn- ->bytes-vec [iid]
  (vec (if (instance? ByteBuffer iid)
         (let [bs (byte-array (.remaining ^ByteBuffer iid))] (.get (.duplicate ^ByteBuffer iid) bs) bs)
         iid)))

(defn- queries [node]
  {:sorted-iids (->> (xt/q node "SELECT _id, _iid FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME ORDER BY _id")
                     (mapv #(update % :xt/iid ->bytes-vec)))

   :history (xt/q node "SELECT *, _valid_from, _valid_to, _system_from, _system_to
                          FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME
                          ORDER BY _id, _system_from, _valid_from")

   :iid-join (tu/query-ra [:group-by '{:columns [{n (row-count)}]}
                           [:join '{:conditions [{_iid r_iid}]}
                            (iid-scan)
                            [:rename '{:columns {_iid r_iid}} (iid-scan)]]]
                          {:node node})

   :iid-intersect (tu/query-ra [:group-by '{:columns [{n (row-count)}]}
                                [:intersect {} (iid-scan) (iid-scan)]]
                               {:node node})})

(defn- patch! [node]
  (xt/execute-tx node ["PATCH INTO readings FOR VALID_TIME FROM TIMESTAMP '2020-01-03T00:00:00Z' RECORDS {_id: 7, extra: 1}"]
                 {:system-time #inst "2021-01-01"})
  (xt/q node "SELECT *, _valid_from, _valid_to FROM readings FOR ALL VALID_TIME WHERE _id = 7 ORDER BY _valid_from"))

(defn- flight-sql-iids
  [^Xtdb node]
  (with-open [al (RootAllocator.)
              flight-client (.build (FlightClient/builder al (Location/forGrpcInsecure "127.0.0.1" (.getFlightSqlPort node))))
              client (FlightSqlClient. flight-client)]
    (let [opts (make-array CallOption 0)
          info (.execute client "SELECT _iid FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME" opts)]
      (with-open [stream (.getStream client (.getTicket ^FlightEndpoint (first (.getEndpoints info))) opts)]
        (let [root (.getRoot stream)]
          (loop [batches []]
            (if (.next stream)
              (let [^FixedSizeBinaryVector iids (.getVector root "_iid")]
                (recur (conj batches {:field (.getField iids)
                                      :iids (mapv #(vec (.getObject iids (int %))) (range (.getRowCount root)))})))
              {:declared (.findField (.getSchema info) "_iid")
               :batch-fields (into #{} (map :field) batches)
               :batches (count batches)
               :iids (sort (mapcat :iids batches))})))))))

(defn- pgwire-iids [^Xtdb node]
  (->> (xt/q (xt/client {:port (.getServerPort node)})
             "SELECT _id, _iid FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME ORDER BY _id")
       (mapv #(update % :xt/iid ->bytes-vec))))

(t/deftest a-scan-over-plain-and-run-end-encoded-iid-pages-answers-as-over-plain-ones-6258
  (binding [cat/*file-size-target* (* 16 1024)
            c/*ignore-signal-block?* true]
    (let [control-dir (util/->path "target/mixed-iid-encoding/control")
          mixed-dir (util/->path "target/mixed-iid-encoding/mixed")
          opts {:rows-per-block 200}]
      (util/delete-dir control-dir)
      (util/delete-dir mixed-dir)

      (doseq [dir [control-dir mixed-dir]]
        (with-open [node (tu/->local-node (assoc opts :node-dir dir))]
          (ingest! node (range 40))))

      (let [rewritten (with-open [al (RootAllocator.)]
                        (into {} (for [f (compacted-data-files mixed-dir)]
                                   [(trie-key f) (rewrite-iid-as-runs! al f)])))
            {:keys [rows runs]} (apply merge-with + (vals rewritten))]
        (t/is (< runs rows) "the rewritten pages carry multi-row runs")

        (with-open [control (tu/->local-node (assoc opts :node-dir control-dir))
                    mixed (tu/->local-node (assoc opts :node-dir mixed-dir))]
          (doseq [node [control mixed]]
            (ingest! node (range 40 70)))

          (let [live (->> (xt/q mixed "SELECT trie_key FROM xt.trie_stats WHERE table_name = 'readings' AND trie_state = 'live' AND level > 0")
                          (into #{} (map :trie-key)))]
            (t/is (some (set (keys rewritten)) live) "a run-encoded file is still live")
            (t/is (some (complement (set (keys rewritten))) live) "a plain compacted file is live alongside it"))

          (let [expected (queries control)
                actual (queries mixed)]
            (doseq [k (keys expected)]
              (t/is (= (get expected k) (get actual k)) (str k))))

          (let [expected (pgwire-iids control)]
            (t/is (seq expected))
            (t/is (= expected (pgwire-iids mixed)) "pgwire"))

          (t/is (= (patch! control) (patch! mixed)) "PATCH")

          (let [{:keys [declared batch-fields] :as expected} (flight-sql-iids control)
                actual (flight-sql-iids mixed)]
            (t/is (= (ArrowType$FixedSizeBinary. 16) (.getType ^Field declared)))
            (t/is (= #{declared} batch-fields (:batch-fields actual)) "every batch is loaded into the one declared field")
            (t/is (< 1 (:batches actual)) "the stream spans several pages")
            (t/is (= (:iids expected) (:iids actual)) "every batch's buffers read back as the iids")))))))
