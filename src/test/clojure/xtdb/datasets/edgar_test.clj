(ns xtdb.datasets.edgar-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.mirror :as mirror]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.log :as xt-log]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.time Duration Instant LocalDate]))

;; The fixture is Apple's FY2024 Q1 10-Q (accn 0000320193-24-000006, filed
;; 2024-02-02), cut from the 2025q4 Financial Statement Data Set. One filing, so
;; no restatement vintages — we exercise the end-to-end transit load path and the
;; single-vintage demo queries.
(def ^:private aapl-cik "0000320193")
(def ^:private q1-end (LocalDate/parse "2023-12-31"))

(def ^:private ^:dynamic *node* nil)

(defn- sample [path] (io/resource (str "edgar/sample/tsv/" path)))

(defn- mirror-fixture!
  "Curate the TSV fixture to a one-quarter transit dataset, the shape the loader
   reads off S3. (mirror! itself fetches from SEC; here we use its local seam to
   write the same transit from the committed fixture readers.)"
  [out-dir]
  (let [transit (io/file out-dir "transit")]
    (with-open [sub (parse/gz-reader (io/file (sample "sub.txt.gz")))
                num (parse/gz-reader (io/file (sample "num.txt.gz")))]
      (mirror/write-quarter-transit! sub num (io/file transit "2025q4.transit.json.gz")))
    (edgar/dataset transit)))

(t/use-fixtures :once
  (fn [f]
    (let [node-dir (util/->path "target/edgar-test")
          mirror-dir (io/file "target/edgar-test-mirror")]
      (util/delete-dir node-dir)
      (util/delete-dir (util/->path "target/edgar-test-mirror"))
      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (binding [*node* node]
          (edgar/submit-edgar! node (mirror-fixture! mirror-dir))
          (xt-log/sync-node node (Duration/ofMinutes 1))
          (tu/flush-block! node)
          (f))))))

(defn- net-income-as-of-system-time [t]
  (-> (xt/q *node* [edgar/q-income-as-of-system-time t aapl-cik q1-end])
      first :net-income-loss))

(t/deftest test-income-loads-via-transit
  ;; the income figure is readable at a system-time after its filing date.
  (t/is (= 33916000000M
           (net-income-as-of-system-time (Instant/parse "2024-03-01T00:00:00Z")))))

(t/deftest test-income-restatement-history
  ;; a single filing → one vintage on the restatement trail.
  (t/is (= [{:net-income-loss 33916000000M :form "10-Q" :filed #xt/date "2024-02-02"}]
           (->> (xt/q *node* [edgar/q-income-restatement-history aapl-cik q1-end])
                (mapv #(select-keys % [:net-income-loss :form :filed]))))))

(defn- assets-as-of-valid-time [d]
  (-> (xt/q *node* [edgar/q-balance-as-of-valid-time d aapl-cik])
      first :assets))

(t/deftest test-balance-valid-time
  ;; instant balance carries valid-from = its period end (2023-12-31), so it's in
  ;; force as-of a later real-world date.
  (t/is (= 353514000000M
           (assets-as-of-valid-time (Instant/parse "2024-01-01T00:00:00Z")))))

(t/deftest test-issuer
  (t/is (= [{:cik aapl-cik :entity-name "APPLE INC"}]
           (xt/q *node* [edgar/q-issuer aapl-cik]))))

(t/deftest test-parse-statement-shape
  (with-open [in (-> (io/input-stream (io/file "target/edgar-test-mirror/transit/2025q4.transit.json.gz"))
                     java.util.zip.GZIPInputStream.)]
    (let [docs (parse/observations->docs (edgar/read-records in))
          by-table (group-by #(:table (meta %)) docs)]
      (t/is (= 1 (count (:issuer by-table))) "one issuer")
      (t/is (pos? (count (:income_statement by-table))) "income rows pivoted")
      (t/is (pos? (count (:balance_sheet by-table))) "balance rows pivoted"))))
