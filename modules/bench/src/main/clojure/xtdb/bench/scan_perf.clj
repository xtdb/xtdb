(ns xtdb.bench.scan-perf
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [taoensso.tufte :as tufte :refer [p]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.compactor :as c]
            [xtdb.datasets.scan-items :as scan-items]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration)))

(defn- get-conn ^java.sql.Connection [^xtdb.api.DataSource node]
  (.build (.createConnectionBuilder node)))

(defn- ?s [n]
  (str "(" (str/join ", " (repeat n "?")) ")"))

(defn- or-clause [col n]
  (str/join " OR " (repeat n (str col " = ?"))))

(defn get-distributed-items
  "Fetch 1000 spread and 1000 clustered items for scan benchmarking."
  [conn]
  (let [n 1000
        cnt (:cnt (first (xt/q conn ["SELECT count(*) as cnt FROM items"])))
        step (max (quot cnt n) 1)
        mid (quot cnt 2)

        cols "_id, item$content_key, item$lang, item$length, item$published_at, item$author_name"

        spread-1000 (vec (xt/q conn
                               [(str "SELECT " cols " FROM ("
                                     "  SELECT " cols ", row_number() OVER (ORDER BY _id) as rn"
                                     "  FROM items"
                                     ") AS numbered WHERE (rn - 1) % ? = 0"
                                     "  ORDER BY rn"
                                     "  LIMIT ?")
                                step n]))

        clustered-1000 (vec (xt/q conn
                                  [(str "SELECT " cols " FROM ("
                                        "  SELECT " cols ", row_number() OVER (ORDER BY _id) as rn"
                                        "  FROM items"
                                        ") AS numbered WHERE rn BETWEEN ? AND ?"
                                        "  ORDER BY rn")
                                   mid (+ mid n -1)]))]
    {:spread-10 (vec (take-nth 100 spread-1000))
     :spread-100 (vec (take-nth 10 spread-1000))
     :spread-1000 spread-1000
     :clustered-10 (vec (take 10 clustered-1000))
     :clustered-100 (vec (take 100 clustered-1000))
     :clustered-1000 clustered-1000
     :langs (vec (distinct (map :item/lang spread-1000)))}))

(defn scan-items-point-benchmark-queries
  "Individual point lookups at each 10% position in the index."
  [{:keys [spread-10]}]
  (for [[i id] (map-indexed vector spread-10)]
    {:id (keyword (str "scan-p" (* i 10)))
     :f #(xt/q % ["SELECT _id FROM items WHERE _id = ?" id])}))

(defn scan-items-set-benchmark-queries
  [{:keys [spread-10 spread-100 spread-1000 clustered-10 clustered-100 clustered-1000]}]
  (let [ids #(mapv :xt/id %)]
    [{:id :scan-spread-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 10))] (ids spread-10)))}
     {:id :scan-clustered-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 10))] (ids clustered-10)))}
     {:id :scan-spread-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "_id" 10))] (ids spread-10)))}
     {:id :scan-clustered-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "_id" 10))] (ids clustered-10)))}

     {:id :scan-spread-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 100))] (ids spread-100)))}
     {:id :scan-clustered-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 100))] (ids clustered-100)))}

     {:id :scan-spread-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 1000))] (ids spread-1000)))}
     {:id :scan-clustered-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE _id IN " (?s 1000))] (ids clustered-1000)))}]))

(defn scan-items-by-content-key-queries
  [{:keys [spread-10 spread-100 spread-1000 clustered-10 clustered-100 clustered-1000]}]
  (let [ckeys #(mapv :item/content-key %)]
    [{:id :content-key-spread-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 10))] (ckeys spread-10)))}
     {:id :content-key-clustered-in-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 10))] (ckeys clustered-10)))}
     {:id :content-key-spread-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "item$content_key" 10))] (ckeys spread-10)))}
     {:id :content-key-clustered-or-10
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE " (or-clause "item$content_key" 10))] (ckeys clustered-10)))}

     {:id :content-key-spread-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 100))] (ckeys spread-100)))}
     {:id :content-key-clustered-in-100
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 100))] (ckeys clustered-100)))}

     {:id :content-key-spread-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 1000))] (ckeys spread-1000)))}
     {:id :content-key-clustered-in-1000
      :f #(xt/q % (into [(str "SELECT _id FROM items WHERE item$content_key IN " (?s 1000))] (ckeys clustered-1000)))}]))

(defn scan-items-filter-queries
  "Selective filter queries across different types and selectivities.
   Targets ~0.05-12% selectivity so results reflect actual filter/pushdown behaviour
   rather than full table scan I/O."
  [{:keys [spread-1000 langs]}]
  (let [lengths (sort (mapv :item/length spread-1000))
        n-samples (count lengths)
        ;; Pick a length value at the 99th percentile for a narrow BETWEEN (~1%)
        len-p99 (nth lengths (int (* 0.99 n-samples)))
        len-p98 (nth lengths (int (* 0.98 n-samples)))
        ;; Pick a specific length value from the middle for point equality (~0.05%)
        len-point (nth lengths (quot n-samples 2))

        timestamps (sort (mapv :item/published-at spread-1000))
        ;; Narrow timestamp window: top ~1% of the range
        ts-p99 (nth timestamps (int (* 0.99 n-samples)))
        ts-p98 (nth timestamps (int (* 0.98 n-samples)))

        ;; Pick a single lang for equality (~12.5%)
        lang-single (first langs)

        ;; Pick a single author_name for equality (~1.5%, from pool of 64)
        author-name-single (:item/author-name (first spread-1000))]

    [;; String equality — single lang (~12.5%)
     {:id :lang-eq
      :f #(xt/q % ["SELECT _id FROM items WHERE item$lang = ?" lang-single])}

     ;; String equality — single author_name (~1.5%)
     {:id :author-name-eq
      :f #(xt/q % ["SELECT _id FROM items WHERE item$author_name = ?" author-name-single])}

     ;; Integer point equality (~0.05%)
     {:id :length-eq
      :f #(xt/q % ["SELECT _id FROM items WHERE item$length = ?" len-point])}

     ;; Integer narrow range (~1%)
     {:id :length-range-1pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$length BETWEEN ? AND ?" len-p98 len-p99])}

     ;; Timestamp narrow range (~1%)
     {:id :published-range-1pct
      :f #(xt/q % ["SELECT _id FROM items WHERE item$published_at BETWEEN ? AND ?" ts-p98 ts-p99])}

     ;; Compound: string + integer (~6%)
     {:id :lang-eq-and-length-gt
      :f #(xt/q % ["SELECT _id FROM items WHERE item$lang = ? AND item$length > ?" lang-single len-p99])}

     ;; Compound: string + timestamp (~0.75%)
     {:id :author-and-published-gt
      :f #(xt/q % ["SELECT _id FROM items WHERE item$author_name = ? AND item$published_at > ?" author-name-single ts-p99])}

     ;; Sparse column (~0.1%)
     {:id :doc-type-not-null
      :f #(xt/q % ["SELECT _id FROM items WHERE item$doc_type IS NOT NULL"])}]))

(defn- profile-data [pstats]
  (let [rows (for [[id {:keys [n min p50 p90 p95 p99 max mean mad sum]}]
                   (get pstats :stats {})]
               {:id id :n n :min min :p50 p50 :p90 p90 :p95 p95 :p99 p99
                :max max :mean mean :mad mad :sum sum})]
    (sort-by :sum > rows)))

(defn- profile [node iterations suite]
  (with-open [conn (get-conn node)]
    (let [_warmup (doseq [{:keys [f]} suite] (f conn))
          [_ pstats] (tufte/profiled
                      {}
                      (doseq [{:keys [id f]} suite
                              _ (range iterations)]
                        (p id (f conn))))]
      {:profile (profile-data @pstats)
       :formatted (tufte/format-pstats pstats)})))

(defmethod b/cli-flags :scan-perf [_]
  [["-n" "--n-items N_ITEMS" "Number of items to generate"
    :parse-fn parse-long
    :default 10000000]
   ["-t" "--id-type ID_TYPE" "ID type: uuid, string, keyword"
    :parse-fn keyword
    :default :uuid]
   ["-h" "--help"]])

(defmethod b/->benchmark :scan-perf [_ {:keys [n-items id-type seed no-load?]
                                         :or {n-items 10000000 id-type :uuid seed 0}}]
  (log/info {:n-items n-items :id-type id-type :seed seed :no-load? no-load?})

  {:title "Scan Perf"
   :benchmark-type :scan-perf
   :seed seed
   :parameters {:n-items n-items :id-type id-type :seed seed :no-load? no-load?}
   :->state #(do {:!state (atom {})})
   :tasks [(when-not no-load?
             {:t :do
              :stage :ingest
              :tasks [{:t :call
                       :stage :submit-docs
                       :f (fn [{:keys [node random]}]
                            (with-open [conn (get-conn node)]
                              (scan-items/load-data! conn random n-items id-type)))}
                      {:t :call
                       :stage :await-transactions
                       :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 5)))}
                      {:t :call
                       :stage :flush-block
                       :f (fn [{:keys [node]}] (tu/flush-block! node))}]})

           {:t :call
            :stage :compact
            :f (fn [{:keys [node]}] (c/compact-all! node nil))}

           {:t :call
            :stage :get-query-data
            :f (fn [{:keys [node !state]}]
                 (with-open [conn (get-conn node)]
                   (swap! !state assoc :distributed-items (get-distributed-items conn))))}

           {:t :call
            :stage :profile-scan-points
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 50 (scan-items-point-benchmark-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-points profile)
                   (println formatted)))}

           {:t :call
            :stage :profile-scan-sets
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 50 (scan-items-set-benchmark-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-sets profile)
                   (println formatted)))}

           {:t :call
            :stage :profile-scan-content-key
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 10 (scan-items-by-content-key-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-content-key profile)
                   (println formatted)))}

           {:t :call
            :stage :profile-scan-filters
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [distributed-items]} @!state
                       {:keys [profile formatted]} (profile node 5 (scan-items-filter-queries distributed-items))]
                   (swap! !state update :profiles assoc :scan-filters profile)
                   (println formatted)))}

           {:t :call
            :stage :output-profile-data
            :f (fn [{:keys [!state] :as worker}]
                 (let [{:keys [profiles]} @!state]
                   (b/log-report worker {:profiles profiles})))}]})

(comment
  (try
    (let [random (java.util.Random. 0)
          n-items (* 10 1000 1000)
          id-type :uuid
          dir (util/->path "tmp/scan-perf")
          clear-dir (fn [^java.nio.file.Path path]
                      (when (util/path-exists path)
                        (log/info "Clearing directory" path)
                        (util/delete-dir path)))]
      (println "Clear dir")
      (clear-dir dir)
      (with-open [node (tu/->local-node {:node-dir dir
                                         :instant-src (java.time.InstantSource/system)})
                  conn (get-conn node)]
        (println "Load data")
        (scan-items/load-data! conn random n-items id-type)
        (println "Flush block")
        (b/sync-node node (Duration/ofMinutes 5))
        (tu/flush-block! node)
        (println "Compact")
        (c/compact-all! node nil)
        (println "Get Query Data")
        (let [distributed-items (get-distributed-items conn)]
          (println "Profile Point Lookups")
          (println (:formatted (profile node 50 (scan-items-point-benchmark-queries distributed-items))))
          (println "Profile Scan Sets")
          (println (:formatted (profile node 50 (scan-items-set-benchmark-queries distributed-items))))
          (println "Profile Content Key")
          (println (:formatted (profile node 10 (scan-items-by-content-key-queries distributed-items))))
          (println "Profile Filters")
          (println (:formatted (profile node 5 (scan-items-filter-queries distributed-items)))))))
    (catch Exception e
      (log/error e)))

)
