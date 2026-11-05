(ns xtdb.compactor-recency-branching-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.table :as table]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie-catalog :as cat])
  (:import (java.nio.file Files Path)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.arrow Relation Relation$Loader RunEndEncodedVector)
           (xtdb.metadata PageMetadata)
           (xtdb.storage LocalStorage)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$IidBranch ArrowHashTrie$Leaf ArrowHashTrie$RecencyBranch HashTrie$RecencyBranch$Bucket)))

(defn- table-path ^Path [node table-name]
  (let [db (db/primary-db node)]
    (-> ^LocalStorage (.getBufferPool db)
        (.getRootPath)
        (.resolve (.getTablePath (.slug (.snap (.getTableCatalog db)) (table/->ref table-name)))))))

(def ^:private start (time/->zdt #inst "2020-01-01"))

(defn- load-history! [node]
  (doseq [tick (range 120)]
    (xt/execute-tx node [(into [:put-docs :docs]
                               (for [x (range 40)] {:xt/id x, :v tick}))]
                   {:system-time (.plusHours start tick)}))

  (doseq [n (range 50)]
    (xt/execute-tx node [[:put-docs :docs {:xt/id :solo, :v n}]]
                   {:system-time (.plusSeconds (.plusHours start 130) (* n 30))})))

(defn- historical-trie-keys [node]
  (with-open [s (Files/list (.resolve (table-path node "public/docs") "meta"))]
    (->> (iterator-seq (.iterator s))
         (map #(str (.getFileName ^Path %)))
         (filter #(re-find #"^l0[1-9a-f]-r2" %))
         sort
         vec)))

(defn- leaf-bounds
  "each leaf's data page index, with the tightest recency-branch key above it"
  [^ArrowHashTrie trie]
  (letfn [(walk [node bound]
            (cond
              (nil? node) []
              (instance? ArrowHashTrie$Leaf node) [[(.getDataPageIndex ^ArrowHashTrie$Leaf node) bound]]
              (instance? ArrowHashTrie$IidBranch node) (mapcat #(walk % bound) (.getHashChildren ^ArrowHashTrie$IidBranch node))
              (instance? ArrowHashTrie$RecencyBranch node) (mapcat (fn [^HashTrie$RecencyBranch$Bucket bucket]
                                                                     (walk (.getNode bucket) (min bound (.getMaxRecency bucket))))
                                                                   (.getBuckets ^ArrowHashTrie$RecencyBranch node))))]
    (walk (.getRootNode trie) Long/MAX_VALUE)))

(defn- trie-summary
  [node trie-key]
  (let [tp (table-path node "public/docs")
        meta-path (.resolve (.resolve tp "meta") ^String trie-key)]
    (with-open [al (RootAllocator.)
                meta-ldr (Relation/loader al meta-path)
                meta-rel (Relation. al (.getSchema meta-ldr))
                page-meta (PageMetadata/open al meta-path)
                ^Relation$Loader data-ldr (Relation/loader al (.resolve (.resolve tp "data") ^String trie-key))
                data-rel (Relation. al (.getSchema data-ldr))]
      (.loadNextPage meta-ldr meta-rel)
      (let [nodes (.vectorFor meta-rel "nodes")
            pages (vec (for [[page-idx bound] (leaf-bounds (ArrowHashTrie. nodes))
                             :let [_ (.loadPage data-ldr page-idx data-rel)
                                   recency-vec (.vectorFor data-rel "_recency")]]
                         {:page-idx page-idx
                          :bound bound
                          :max-recency (.maxRecency page-meta page-idx)
                          :newest (reduce max Long/MIN_VALUE
                                          (map #(.getLong recency-vec %) (range (.getRowCount data-rel))))}))]
        {:recency-branched? (contains? (set (.getLegNames nodes)) "branch-recency")
         :bound-violations (vec (for [{:keys [page-idx ^long newest ^long bound]} pages
                                      :when (> newest bound)]
                                  [trie-key page-idx newest bound]))
         :max-recency-mismatches (vec (for [{:keys [page-idx ^long newest max-recency]} pages
                                            :when (not (and max-recency (<= newest (long max-recency) (inc newest))))]
                                        [trie-key page-idx newest max-recency]))}))))

(def ^:private window-sql
  "SELECT _id, v, _system_from FROM docs
   FOR SYSTEM_TIME FROM TIMESTAMP '2020-01-05T19:00:00Z' TO TIMESTAMP '2020-01-06T07:00:00Z'")

(def ^:private all-sql
  "SELECT _id, v, _system_from, _system_to FROM docs FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")

(defn- rows-read [node sql]
  (->> (xt/q node (str "EXPLAIN ANALYZE " sql))
       (filter #(= :scan (:op %)))
       first :attributes :scan-rows-read))

(defn- query-results [node]
  {:window (set (xt/q node window-sql))
   :all (set (xt/q node all-sql))
   :solo (xt/q node ["SELECT v, _system_from FROM docs FOR ALL SYSTEM_TIME WHERE _id = ? ORDER BY _system_from" :solo])
   :row-count (:row-count (first (xt/q node "SELECT COUNT(*) AS row_count FROM docs FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")))})

(t/deftest historical-files-branch-by-recency-and-a-recent-window-reads-a-fraction-of-them
  (tu/with-tmp-dir* "compactor-recency-branching"
    (fn [node-dir]
      (binding [c/*page-size* 8
                cat/*file-size-target* (* 1024 16)
                c/*ignore-signal-block?* true]
        ;; TODO(#6263): the opt-in goes with withinFileRecencyBranching
        (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 400, :within-file-recency-branching? true})]
          (load-history! node)
          (tu/flush-block! node)

          (let [before (query-results node)]
            (t/is (= 50 (count (:solo before))))

            (c/compact-all! node #xt/duration "PT30S")

            (let [trie-keys (historical-trie-keys node)
                  summaries (map #(trie-summary node %) trie-keys)]
              (t/is (some #(re-find #"^l0[2-9a-f]-" %) trie-keys) "rows have compacted past L1H")
              (t/is (some :recency-branched? summaries) "historical files branch on recency")
              (t/is (empty? (mapcat :bound-violations summaries)) "no page holds a row newer than its bucket's bound")
              (t/is (empty? (mapcat :max-recency-mismatches summaries)) "each page's metadata records its newest recency"))

            (t/is (= before (query-results node)) "compaction leaves query results unchanged")

            (t/is (< (* 4 (rows-read node window-sql)) (rows-read node all-sql))
                  "a 12h window reads a fraction of five days' history")

            (t/is (not-any? #(re-find #"recency" (str %))
                            (concat (mapcat keys (xt/q node "SELECT * FROM docs FOR ALL SYSTEM_TIME"))
                                    (map :column-name (xt/q node "SELECT column_name FROM information_schema.columns WHERE table_name = 'docs'"))))
                  "the recency column isn't visible to queries")))))))

(defn- iid-run-counts
  "per data page: its `_iid` runs, and the entities it holds"
  [node trie-key]
  (let [tp (table-path node "public/docs")]
    (with-open [al (RootAllocator.)
                ^Relation$Loader data-ldr (Relation/loader al (.resolve (.resolve tp "data") ^String trie-key))
                data-rel (Relation. al (.getSchema data-ldr))]
      (loop [counts []]
        (if (.loadNextPage data-ldr data-rel)
          (let [^RunEndEncodedVector iid-vec (.vectorFor data-rel "_iid")
                iids (map #(let [iid (.getObject iid-vec %)] (if (bytes? iid) (vec iid) iid))
                          (range (.getRowCount data-rel)))]
            (recur (conj counts [(.getRunCount iid-vec) (count (partition-by identity iids))])))
          counts)))))

(t/deftest recency-branched-pages-keep-each-entitys-iid-in-one-run
  (tu/with-tmp-dir* "compactor-recency-branching-ree"
    (fn [node-dir]
      (binding [c/*page-size* 8
                cat/*file-size-target* (* 1024 16)
                c/*ignore-signal-block?* true]
        ;; TODO(#6263): the opt-in goes with withinFileRecencyBranching
        (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 400
                                           :ree-iids? true, :within-file-recency-branching? true})]
          (load-history! node)
          (tu/flush-block! node)
          (c/compact-all! node #xt/duration "PT30S")

          (let [trie-keys (historical-trie-keys node)]
            (t/is (some #(:recency-branched? (trie-summary node %)) trie-keys) "historical files branch on recency")

            (t/is (empty? (for [trie-key trie-keys
                                [runs entities] (iid-run-counts node trie-key)
                                :when (not= runs entities)]
                            [trie-key runs entities]))
                  "every page stores each entity's iid in one run")))))))

(t/deftest deleted-and-erased-entities-compact-through-recency-cells
  (tu/with-tmp-dir* "compactor-recency-branching-erase"
    (fn [node-dir]
      (binding [c/*page-size* 8
                cat/*file-size-target* (* 1024 16)
                c/*ignore-signal-block?* true]
        ;; TODO(#6263): the opt-in goes with withinFileRecencyBranching
        (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 400, :within-file-recency-branching? true})]
          (load-history! node)
          (xt/execute-tx node [(into [:delete-docs :docs] (range 0 5))]
                         {:system-time (.plusHours start 140)})
          (xt/execute-tx node [(into [:erase-docs :docs] (cons :solo (range 5 10)))]
                         {:system-time (.plusHours start 141)})
          (tu/flush-block! node)

          (let [before (query-results node)]
            (t/is (empty? (:solo before)))
            (t/is (not-any? (comp #{5 6 7 8 9 :solo} :xt/id) (:window before))
                  "a window before the erase holds none of the erased entities")

            (c/compact-all! node #xt/duration "PT30S")

            (t/is (some #(:recency-branched? (trie-summary node %)) (historical-trie-keys node)) "historical files branch on recency")

            (t/is (= before (query-results node)) "compaction leaves query results unchanged")

            (t/is (= [{:row-count 0}]
                     (xt/q node "SELECT COUNT(*) AS row_count FROM docs FOR ALL SYSTEM_TIME FOR ALL VALID_TIME WHERE _id IN (5, 6, 7, 8, 9)"))
                  "erased entities leave no rows")

            (t/is (= 120 (count (xt/q node "SELECT v FROM docs FOR ALL SYSTEM_TIME WHERE _id = 0"))))))))))
