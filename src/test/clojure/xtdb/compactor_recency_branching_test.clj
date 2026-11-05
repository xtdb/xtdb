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
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf)))

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
      (let [carries-recency? (some? (.vectorForOrNull data-rel "_recency"))
            pages (when carries-recency?
                    (vec (for [^ArrowHashTrie$Leaf leaf (.getLeaves (ArrowHashTrie. (.vectorFor meta-rel "nodes")))
                               :let [page-idx (.getDataPageIndex leaf)
                                     _ (.loadPage data-ldr page-idx data-rel)
                                     recency-vec (.vectorFor data-rel "_recency")]]
                           {:page-idx page-idx
                            :max-recency (.maxRecency page-meta page-idx)
                            :newest (reduce max Long/MIN_VALUE
                                            (map #(.getLong recency-vec %) (range (.getRowCount data-rel))))})))]
        {:carries-recency? carries-recency?
         :max-recency-mismatches (vec (for [{:keys [page-idx ^long newest max-recency]} pages
                                            :when (not (and max-recency (<= newest (long max-recency) (inc newest))))]
                                        [trie-key page-idx newest max-recency]))}))))

(def ^:private window-sql
  "SELECT _id, v, _system_from FROM docs
   FOR SYSTEM_TIME FROM TIMESTAMP '2020-01-05T19:00:00Z' TO TIMESTAMP '2020-01-06T07:00:00Z'")

(def ^:private all-sql
  "SELECT _id, v, _system_from, _system_to FROM docs FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")

(defn- query-results [node]
  {:window (set (xt/q node window-sql))
   :all (set (xt/q node all-sql))
   :solo (xt/q node ["SELECT v, _system_from FROM docs FOR ALL SYSTEM_TIME WHERE _id = ? ORDER BY _system_from" :solo])
   :row-count (:row-count (first (xt/q node "SELECT COUNT(*) AS row_count FROM docs FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")))})

(t/deftest historical-files-carry-recency-and-read-back-identically
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
              (t/is (every? :carries-recency? summaries) "every historical file carries its rows' recency")
              (t/is (empty? (mapcat :max-recency-mismatches summaries)) "each page's metadata records its newest recency"))

            (t/is (= before (query-results node)) "compaction leaves query results unchanged")

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

(t/deftest pages-carrying-recency-keep-each-entitys-iid-in-one-run
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
            (t/is (every? #(:carries-recency? (trie-summary node %)) trie-keys) "every historical file carries its rows' recency")

            (t/is (empty? (for [trie-key trie-keys
                                [runs entities] (iid-run-counts node trie-key)
                                :when (not= runs entities)]
                            [trie-key runs entities]))
                  "every page stores each entity's iid in one run")))))))
