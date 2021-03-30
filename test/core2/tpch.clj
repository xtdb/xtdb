(ns core2.tpch
  (:require [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import [io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable]
           [java.nio.file Files LinkOption]
           [java.time Clock Duration ZoneId]))

(def table->pkey
  {"part" [:p_partkey]
   "supplier" [:s_suppkey]
   "partsupp" [:ps_partkey :ps_suppkey]
   "customer" [:c_custkey]
   "lineitem" [:l_orderkey :l_linenumber]
   "orders" [:o_orderkey]
   "nation" [:n_nationkey]
   "region" [:r_regionkey]})

(defn tpch-entity->pkey-doc [^TpchTable t ^TpchEntity b]
  (let [doc (->> (for [^TpchColumn c (.getColumns t)]
                   [(keyword (.getColumnName c))
                    (condp = (.getBase (.getType c))
                      TpchColumnType$Base/IDENTIFIER
                      (str (str/replace (.getColumnName c) #".+_" "") "_" (.getIdentifier c b))
                      TpchColumnType$Base/INTEGER
                      (long (.getInteger c b))
                      TpchColumnType$Base/VARCHAR
                      (.getString c b)
                      TpchColumnType$Base/DOUBLE
                      (.getDouble c b)
                      TpchColumnType$Base/DATE
                      (inst/read-instant-date (GenerateUtils/formatDate (.getDate c b))))])
                 (into {}))
        table-name (.getTableName t)
        pkey-columns (get table->pkey table-name)
        pkey (mapv doc pkey-columns)]
    (assoc doc
           :_id (str/join "___" pkey)
           :_table table-name)))

(def default-scale-factor 0.05)

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
(defn tpch-table->docs
  ([^TpchTable table]
   (tpch-table->docs table default-scale-factor))
  ([^TpchTable table scale-factor]
   (for [doc (.createGenerator table scale-factor 1 1)]
     (tpch-entity->pkey-doc table doc))))

(defn submit-docs!
  ([tx-producer]
   (submit-docs! tx-producer default-scale-factor))
  ([tx-producer scale-factor]
   (log/debug "Transacting TPC-H tables...")
   (->> (TpchTable/getTables)
        (reduce (fn [_last-tx ^TpchTable t]
                  (let [[last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_last-tx last-doc-count] batch]
                                                           [@(c2/submit-tx tx-producer
                                                                           (vec (for [doc batch]
                                                                                  {:op :put, :doc doc})))
                                                            (+ last-doc-count (count batch))])
                                                         [nil 0]))]
                    (log/debug "Transacted" doc-count (.getTableName t))
                    last-tx))
                nil))))

(def test-sf 0.001)
(assert (contains? #{0.001 0.01} test-sf))
;; 0.001 : "Elapsed time: 2421.146644 msecs"
;; 0.01  : "Elapsed time: 27356.329358 msecs"

(t/deftest ^:integration can-submit-tpch-docs
  (let [node-dir (util/->path "target/can-submit-tpch-docs")
        objects-dir (.resolve node-dir "objects")
        mock-clock (Clock/fixed (.toInstant #inst "2021-04-01") (ZoneId/of "UTC"))]
    (util/delete-dir node-dir)

    (time
     (with-open [node (c2/->local-node node-dir)
                 tx-producer (c2/->local-tx-producer node-dir {:clock mock-clock})]
       (let [last-tx (submit-docs! tx-producer test-sf)]
         (c2/await-tx node last-tx (Duration/ofMinutes 1))

         (tu/finish-chunk node)
         (t/is (= (case test-sf 0.001 67, 0.01 225)
                  (count (iterator-seq (.iterator (Files/list objects-dir)))))))))

    (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))

    (let [expected-dir (.toPath (io/as-file (io/resource (format "can-submit-tpch-docs-%s/" test-sf))))]
      (doseq [expected-path (iterator-seq (.iterator (Files/list expected-dir)))
              :let [actual-path (.resolve objects-dir (.relativize expected-dir expected-path))]]
        (t/is (Files/exists actual-path (make-array LinkOption 0)))
        (tu/check-json-file expected-path actual-path)))))
