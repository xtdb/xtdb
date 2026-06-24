(ns xtdb.datasets.edgar-tsv-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.datasets.edgar.mirror :as mirror]
            [xtdb.datasets.edgar.parse :as parse])
  (:import [java.time LocalDate]))

;; Fixtures are real rows for Apple's FY2024 Q1 10-Q (accn 0000320193-24-000006),
;; cut from the 2025q4 Financial Statement Data Set: sub.txt header + that filing,
;; num.txt header + that filing's facts.
(defn- sample [path] (io/resource (str "edgar/sample/tsv/" path)))

(defn- docs []
  (with-open [sub (parse/gz-reader (io/file (sample "sub.txt.gz")))
              num (parse/gz-reader (io/file (sample "num.txt.gz")))]
    (parse/observations->docs (mirror/quarter->observations sub num))))

(defn- by-table [docs] (group-by #(:table (meta %)) docs))

(t/deftest test-tsv-pivots-statements
  (let [{:keys [issuer income_statement balance_sheet]} (by-table (docs))]
    ;; EDGAR sub.txt carries the registrant name uppercased.
    (t/is (= [{:cik "0000320193" :entity-name "APPLE INC"}]
             (map #(select-keys % [:cik :entity-name]) issuer))
          "one issuer, from the filing's name")

    ;; Doc keys are the snake-cased concept verbatim (:net_income_loss); the
    ;; SQL→kebab mapping only happens on XT query results, not in the parse layer.
    ;; income_statement: current quarter ddate=2023-12-31, qtrs=1 → a one-quarter
    ;; duration ending then, so period_start = 2023-09-30.
    (let [q1 (->> income_statement
                  (filter #(= (LocalDate/parse "2023-12-31") (:period-end %)))
                  first)]
      (t/is (= (LocalDate/parse "2023-09-30") (:period-start q1))
            "qtrs=1 derives a one-quarter start")
      (t/is (= 33916000000M (:net_income_loss q1)))
      (t/is (= 54855000000M (:gross_profit q1))))

    ;; balance_sheet: instant as-of 2023-12-31 (no period_start).
    (let [bs (->> balance_sheet
                  (filter #(= (LocalDate/parse "2023-12-31") (:period-end %)))
                  first)]
      (t/is (nil? (:period-start bs)) "instant facts have no start")
      (t/is (= 353514000000M (:assets bs)))
      (t/is (= 15460223000M (:common_stock_shares_outstanding bs))))))

(t/deftest test-tsv-accession-and-filed
  ;; every doc carries the filing's accession + filed (the atomic-transaction key).
  (t/is (every? #(and (= "0000320193-24-000006" (:accession %))
                      (= (LocalDate/parse "2024-02-02") (:filed %)))
                (filter #(= :income_statement (:table (meta %))) (docs)))))
