(ns xtdb.multi-partition-scan-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (xtdb.api TransactionKey)
           (xtdb.api.query PrepareOpts QueryOpts)
           (xtdb.catalog DatabaseTableCatalog)
           (xtdb.database Database Database$Catalog DatabasePartition)
           (xtdb.indexer DatabaseSnapshot)
           (xtdb.query IQuerySource IQuerySource$QueryCatalog IQuerySource$QueryDatabase PreparedQuery)))

(t/use-fixtures :each tu/with-mock-clock tu/with-allocator)

;; Multi-partition `Database`s aren't constructible yet — `Database.open` builds partition 0 and the
;; attach-time gate rejects anything else until #5837. Two attached databases on one node stand in:
;; each contributes a partition's worth of real storage, live index and trie catalog, written to
;; through the public API, and the two share the node's root allocator (which two nodes wouldn't —
;; the scan transfers page ownership, and Arrow refuses that across roots).
;;
;; The scan can't tell the difference: everything it reads hangs off the QueryPartition and the
;; DatabaseSnapshot slot beside it.
(defn- ->partitioned-db-cat ^IQuerySource$QueryCatalog [node db-names]
  (let [^Database$Catalog db-cat (db/<-node node)
        parts (mapv #(first (.getPartitions ^Database (.databaseOrNull db-cat %))) db-names)
        table-cat (DatabaseTableCatalog. (mapv #(.getTableCatalog ^DatabasePartition %) parts))
        qdb (reify IQuerySource$QueryDatabase
              (getName [_] "xtdb")
              (getPartitions [_] parts)
              (getTableCatalog [_] table-cat)
              (openSnapshot [_ min-basis]
                (DatabaseSnapshot. (into [] (map-indexed (fn [idx ^DatabasePartition part]
                                                           (.openSnapshot part (when min-basis (nth min-basis idx nil)))))
                                         parts))))]
    (reify IQuerySource$QueryCatalog
      (getDatabaseNames [_] ["xtdb"])
      (databaseOrNull [_ n] (when (= n "xtdb") qdb)))))

;; each partition's latest-completed system time, as the read basis the presented database would
;; publish. Without it the live index serves its cached snapshot, which predates the writes.
(defn- ->snapshot-token [node db-names]
  (let [^Database$Catalog db-cat (db/<-node node)]
    (basis/->time-basis-str
     {"xtdb" (mapv #(first (.currentBasis ^Database (.databaseOrNull db-cat %))) db-names)})))

;; `prepareRa` directly, rather than `tu/query-ra`, because the catalog isn't the node's own — hence
;; also the explicit sync and basis resolution, which `tu/query-ra` would otherwise do for us.
(defn- query-ra
  ([node db-names plan] (query-ra node db-names plan nil))
  ([node db-names plan snapshot-token]
   (xt-log/sync-node node #xt/duration "PT5S")

   (let [snapshot-token (or snapshot-token (->snapshot-token node db-names))
         ^IQuerySource q-src (.getQuerySource (util/node-base node))
         ^PreparedQuery pq (.prepareRa q-src plan (->partitioned-db-cat node db-names)
                                       (PrepareOpts. nil nil "xtdb" nil false false snapshot-token))]
     (util/with-open [args (.openSlice vw/empty-args tu/*allocator*)
                      res (.openQuery pq args (QueryOpts. nil nil snapshot-token nil))]
       (into [] cat (tu/<-cursor res))))))

(def ^:private db-names ["xtdb" "part1"])

(defn- start-node ^xtdb.api.Xtdb []
  (doto (xtn/start-node tu/*node-opts*)
    (jdbc/execute! ["ATTACH DATABASE part1"])))

(t/deftest scan-unions-every-partition
  (with-open [node (start-node)]
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :a, :v 1}]])
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :b, :v 2}]
                         [:put-docs :xt_docs {:xt/id :c, :v 3}]]
                   {:database :part1})

    (t/is (= #{{:xt/id :a, :v 1} {:xt/id :b, :v 2} {:xt/id :c, :v 3}}
             (set (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [_id v]}]))))))

(t/deftest scan-unions-historical-and-live-across-partitions
  (with-open [node (start-node)]
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :flushed, :v 1}]])
    (tu/flush-block! node)
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :live, :v 2}]] {:database :part1})

    (t/is (= #{{:xt/id :flushed, :v 1} {:xt/id :live, :v 2}}
             (set (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [_id v]}])))
          "one partition's tries and another's live index both reach the same scan")))

(t/deftest column-types-merge-across-partitions
  (with-open [node (start-node)]
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id 0, :v 1}]])
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id 1, :v "foo"}]] {:database :part1})

    (t/testing "live types, merged off the partitions' snapshots"
      (t/is (= #{{:v 1} {:v "foo"}}
               (set (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [v]}])))
            "an i64 in one partition and a utf8 in another widen to their union rather than one losing rows"))

    (t/testing "historical types, merged off the partitions' table catalogs"
      (tu/flush-block! node)

      (t/is (= #{{:v 1} {:v "foo"}}
               (set (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [v]}])))))))

(t/deftest column-absent-from-one-partition-widens-to-nullable
  (with-open [node (start-node)]
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id 0, :v 1}]])
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id 1}]] {:database :part1})
    (tu/flush-block! node)

    (t/is (= #{{:xt/id 0, :v 1} {:xt/id 1}}
             (set (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [_id v]}])))
          "a partition that has the table but not the column contributes an absent `v`, not nothing")))

(t/deftest each-partition-applies-its-own-temporal-bound
  (with-open [node (start-node)]
    (let [scan '[:scan {:db-name "xtdb", :table #xt/table xt_docs, :columns [_id]}]
          put (fn [id & {:as opts}]
                (.getSystemTime ^TransactionKey (xt/execute-tx node [[:put-docs :xt_docs {:xt/id id}]] opts)))

          ;; interleaved, so each partition's later tx lands inside the *other* partition's span: a
          ;; scan that read partition 0's basis slot for every branch would let "1-late" through
          _0-early (put "0-early")
          t1-early (put "1-early" {:database :part1})
          _1-late (put "1-late" {:database :part1})
          t0-late (put "0-late")]

      (t/is (= #{"0-early" "0-late" "1-early" "1-late"} (set (map :xt/id (query-ra node db-names scan))))
            "sanity: every tx of both partitions is visible with no basis pinned")

      (t/is (= #{"0-early" "0-late" "1-early"}
               (set (map :xt/id (query-ra node db-names scan
                                          (basis/->time-basis-str {"xtdb" [t0-late t1-early]})))))
            "partition 1 reads its own earlier basis slot, so its later tx is excluded while partition 0's is not"))))

(t/deftest live-tables-sums-row-counts-across-partitions
  (with-open [node (start-node)]
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :a}]])
    (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :b}]
                         [:put-docs :xt_docs {:xt/id :c}]]
                   {:database :part1})

    (t/is (= [{:schema-name "public", :table-name "xt_docs", :row-count 3}]
             (->> (query-ra node db-names '[:scan {:db-name "xtdb", :table #xt/table xt/live_tables
                                                   :columns [schema_name table_name row_count]}])
                  (filter (comp #{"xt_docs"} :table-name)))))))
