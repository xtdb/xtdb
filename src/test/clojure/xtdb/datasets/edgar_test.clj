(ns xtdb.datasets.edgar-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.log :as xt-log]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.time Duration Instant LocalDate]))

(def ^:private aapl-cik "0000320193")
(def ^:private fy2008-end (LocalDate/parse "2008-09-27"))

(def ^:private ^:dynamic *node* nil)

(defn- sample [path] (io/resource (str "edgar/sample/" path)))

(t/use-fixtures :once
  (fn [f]
    (let [node-dir (util/->path "target/edgar-test")]
      (util/delete-dir node-dir)
      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (binding [*node* node]
          (edgar/submit-edgar! node {:files [(sample "CIK0000320193.json.gz")]})
          (xt-log/sync-node node (Duration/ofMinutes 1))
          (tu/flush-block! node)
          (f))))))

(defn- net-income-as-of-system-time [t]
  (-> (xt/q *node* [edgar/q-income-as-of-system-time t aapl-cik fy2008-end])
      first :net-income-loss))

(t/deftest test-income-restatement-correction
  ;; duration fact on the system-time axis: the prior basis is immutable across
  ;; the later restatement. Apple's FY2008 net income was $4,834M as first filed
  ;; (10-K, 2009-10-27), restated to $6,119M (10-K/A, 2010-01-25).
  (let [before (Instant/parse "2009-11-01T00:00:00Z")
        after (Instant/parse "2010-02-01T00:00:00Z")]
    (t/is (= 4834000000M (net-income-as-of-system-time before)))
    (t/is (= 6119000000M (net-income-as-of-system-time after)))
    (t/is (= 4834000000M (net-income-as-of-system-time before))
          "re-running the earlier basis still yields the original")))

(t/deftest test-income-restatement-history
  ;; both vintages, oldest filing first — the restatement trail.
  (t/is (= [{:net-income-loss 4834000000M :form "10-K" :filed #xt/date "2009-10-27"}
            {:net-income-loss 6119000000M :form "10-K/A" :filed #xt/date "2010-01-25"}]
           (->> (xt/q *node* [edgar/q-income-restatement-history aapl-cik fy2008-end])
                (mapv #(select-keys % [:net-income-loss :form :filed]))))))

(defn- assets-as-of-valid-time [d]
  (-> (xt/q *node* [edgar/q-balance-as-of-valid-time d aapl-cik])
      first :assets))

(t/deftest test-balance-valid-time-timeline
  ;; instant fact on the valid-time axis: a balance is as-of its period end, so a
  ;; later as-of date supersedes the earlier in valid-time. As-of the FY2008
  ;; close we see FY2008 assets; as-of the FY2009 close, FY2009 assets. These read
  ;; at the current system-time, so FY2008 shows the restated figure ($36,171M,
  ;; per the 10-K/A) — the correction lives on the system-time axis, not here.
  (t/is (= 36171000000M (assets-as-of-valid-time (Instant/parse "2008-10-01T00:00:00Z")))
        "FY2008 assets (restated value, as known now)")
  (t/is (= 53851000000M (assets-as-of-valid-time (Instant/parse "2009-10-01T00:00:00Z")))
        "FY2009 assets supersede in valid-time"))

(t/deftest test-issuer
  (t/is (= [{:cik aapl-cik :entity-name "Apple Inc."}]
           (xt/q *node* [edgar/q-issuer aapl-cik]))))

(t/deftest test-parse-statement-shape
  (with-open [rdr (parse/gz-reader (sample "CIK0000320193.json.gz"))]
    (let [docs (parse/read-docs rdr parse/demo-cik-allow-set)
          by-table (group-by #(:table (meta %)) docs)]
      (t/is (= 1 (count (:issuer by-table))) "one issuer")
      (t/is (= 2 (count (:income_statement by-table)))
            "two NetIncomeLoss FY2008 vintages")
      ;; Assets: FY2008 (2 vintages) + FY2009 (1); shares: 2 as-of dates → 5 rows.
      (t/is (= 5 (count (:balance_sheet by-table)))
            "balance rows pivot per (period-end, accession)"))))
