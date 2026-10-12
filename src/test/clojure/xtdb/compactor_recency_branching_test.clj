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
           (xtdb.arrow Relation Relation$Loader)
           (xtdb.storage LocalStorage)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf)))

(defn- table-path ^Path [node table-name]
  (let [db (db/primary-db node)]
    (-> ^LocalStorage (.getBufferPool db)
        (.getRootPath)
        (.resolve (.getTablePath (.slug (.getTableCatalog db) (table/->ref table-name)))))))

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
  "whether the trie branches on recency, and any page holding a row newer than the bound its branches give it"
  [node trie-key]
  (let [tp (table-path node "public/docs")]
    (with-open [al (RootAllocator.)
                meta-ldr (Relation/loader al (.resolve (.resolve tp "meta") ^String trie-key))
                meta-rel (Relation. al (.getSchema meta-ldr))
                ^Relation$Loader data-ldr (Relation/loader al (.resolve (.resolve tp "data") ^String trie-key))
                data-rel (Relation. al (.getSchema data-ldr))]
      (.loadNextPage meta-ldr meta-rel)
      (let [nodes (.vectorFor meta-rel "nodes")]
        {:recency-branched? (contains? (set (.getLegNames nodes)) "branch-recency")
         :bound-violations (vec (for [^ArrowHashTrie$Leaf leaf (.getLeaves (ArrowHashTrie. nodes))
                                      :let [page-idx (.getDataPageIndex leaf)
                                            bound (.getRecencyBound leaf)
                                            _ (.loadPage data-ldr page-idx data-rel)
                                            recency-vec (.vectorFor data-rel "_recency")
                                            newest (reduce max Long/MIN_VALUE
                                                           (map #(.getLong recency-vec %) (range (.getRowCount data-rel))))]
                                      :when (> newest bound)]
                                  [trie-key page-idx newest bound]))}))))

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
   :solo (xt/q node ["SELECT v, _system_from FROM docs FOR ALL SYSTEM_TIME WHERE _id = ? ORDER BY _system_from" :solo])})

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

            (let [summaries (map #(trie-summary node %) (historical-trie-keys node))]
              (t/is (some :recency-branched? summaries) "historical files branch on recency")
              (t/is (empty? (mapcat :bound-violations summaries)) "no page holds a row newer than its bucket's bound"))

            (t/is (= before (query-results node)) "compaction leaves query results unchanged")

            (t/is (< (* 4 (rows-read node window-sql)) (rows-read node all-sql))
                  "a 12h window reads a fraction of five days' history")

            (t/is (not-any? #(re-find #"recency" (str %))
                            (concat (mapcat keys (xt/q node "SELECT * FROM docs FOR ALL SYSTEM_TIME"))
                                    (map :column-name (xt/q node "SELECT column_name FROM information_schema.columns WHERE table_name = 'docs'"))))
                  "the recency column isn't visible to queries")))))))
