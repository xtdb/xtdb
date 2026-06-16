(ns xtdb.datasets.edgar-restatement-test
  "The committed TSV fixture is a single filing, so it can't show a restatement.
   Here we craft two minimal quarters where a later filing re-states an earlier
   period's net income, and assert the correction lands on the system-time axis —
   the demo's centrepiece, end-to-end through mirror → transit → load."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.mirror :as mirror]
            [xtdb.log :as xt-log]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.time Duration Instant LocalDate]))

(def ^:private cik "0000999999")
(def ^:private fy2023-end (LocalDate/parse "2023-12-31"))

(def ^:private sub-header
  ["adsh" "cik" "name" "sic" "countryba" "stprba" "cityba" "zipba" "bas1" "bas2"
   "baph" "countryma" "stprma" "cityma" "zipma" "mas1" "mas2" "countryinc"
   "stprinc" "ein" "former" "changed" "afs" "wksi" "fye" "form" "period" "fy"
   "fp" "filed" "accepted" "prevrpt" "detail" "instance" "nciks" "aciks"])

(defn- sub-row [adsh form filed]
  ;; only the columns the loader reads need real values; the rest are padded blank.
  (let [m {"adsh" adsh "cik" "999999" "name" "ACME CORP" "form" form
           "period" "20231231" "fy" "2023" "fp" "FY" "filed" filed
           "accepted" (str (subs filed 0 4) "-" (subs filed 4 6) "-" (subs filed 6 8) " 12:00:00.0")}]
    (mapv #(get m % "") sub-header)))

(def ^:private num-header
  ["adsh" "tag" "version" "ddate" "qtrs" "uom" "segments" "coreg" "value" "footnote"])

(defn- num-row [adsh value]
  ;; NetIncomeLoss for FY2023 (qtrs=4 → start 2023-01-01), consolidated.
  [adsh "NetIncomeLoss" "us-gaap/2023" "20231231" "4" "USD" "" "" value ""])

(defn- tsv-reader [header rows]
  (io/reader (.getBytes (str (str/join "\t" header) "\n"
                             (str/join "\n" (map #(str/join "\t" %) rows)) "\n")
                        "UTF-8")))

(defn- write-quarter! [transit-dir q adsh form filed value]
  ;; mirror! fetches from SEC; here we craft sub/num readers and use mirror's
  ;; local seam to write the same per-quarter transit the loader reads.
  (with-open [sub (tsv-reader sub-header [(sub-row adsh form filed)])
              num (tsv-reader num-header [(num-row adsh value)])]
    (mirror/write-quarter-transit! sub num (io/file transit-dir (str q ".transit.json.gz")))))

(def ^:private ^:dynamic *node* nil)

(t/use-fixtures :once
  (fn [f]
    (let [base (io/file "target/edgar-restatement-test")
          transit (io/file base "transit")
          node-dir (util/->path "target/edgar-restatement-node")]
      (util/delete-dir (util/->path (.getPath base)))
      (util/delete-dir node-dir)
      ;; q1 reports FY2023 net income = 1000 (filed 2024-02-02);
      ;; q2 re-states it to 1200 via a 10-K/A (filed 2024-08-01).
      (write-quarter! transit "2024q1" "0000999999-24-000001" "10-K" "20240202" "1000")
      (write-quarter! transit "2024q3" "0000999999-24-000009" "10-K/A" "20240801" "1200")
      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (binding [*node* node]
          (edgar/submit-edgar! node (edgar/dataset transit))
          (xt-log/sync-node node (Duration/ofMinutes 1))
          (tu/flush-block! node)
          (f))))))

(defn- net-income-as-of [t]
  (-> (xt/q *node* [edgar/q-income-as-of-system-time t cik fy2023-end])
      first :net-income-loss))

(t/deftest test-restatement-on-system-time
  ;; before the restatement was filed: the original $1000; after: $1200; and the
  ;; earlier basis stays immutable when re-queried.
  (let [before (Instant/parse "2024-03-01T00:00:00Z")
        after (Instant/parse "2024-09-01T00:00:00Z")]
    (t/is (= 1000M (net-income-as-of before)))
    (t/is (= 1200M (net-income-as-of after)))
    (t/is (= 1000M (net-income-as-of before))
          "re-running the earlier basis still yields the original")))

(t/deftest test-restatement-history
  (t/is (= [{:net-income-loss 1000M :form "10-K" :filed #xt/date "2024-02-02"}
            {:net-income-loss 1200M :form "10-K/A" :filed #xt/date "2024-08-01"}]
           (->> (xt/q *node* [edgar/q-income-restatement-history cik fy2023-end])
                (mapv #(select-keys % [:net-income-loss :form :filed]))))))
