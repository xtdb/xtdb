(ns xtdb.datasets.edgar.tsv
  "Reads SEC EDGAR 'Financial Statement Data Sets' — the quarterly TSV bulk
   dumps — into the same statement docs the companyfacts front-end produces
   (xtdb.datasets.edgar.parse). Where companyfacts is cut by company (one file,
   full history), the TSVs are cut by time (one quarter, every filer), so this is
   the breadth/streaming source for the Postgres → XT demo.

   A quarter ZIP holds, among others:
   - sub.txt — one row per filing: adsh, cik, name, form, period, fy, fp, filed,
     accepted (system-time, to the second).
   - num.txt — one row per fact: adsh, tag, version, ddate (period end), qtrs
     (0 = instant balance, else a duration spanning that many quarters), uom,
     segments, coreg, value.

   We join num→sub on adsh, keep only consolidated rows (blank segments/coreg)
   whose (taxonomy, tag) is registered, derive each fact's period from
   ddate/qtrs, and reuse the shared pivot to make the wide statement docs."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.serde :as serde])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

;; EDGAR TSV dates are yyyymmdd (no separators), distinct from companyfacts ISO.
(def ^:private ^DateTimeFormatter tsv-date-fmt (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn- parse-tsv-date ^LocalDate [s]
  (when-not (str/blank? s)
    (try (LocalDate/parse s tsv-date-fmt)
         (catch DateTimeParseException _ nil))))

(defn- version->taxonomy
  "num.txt `version` is either a standard taxonomy like 'us-gaap/2024' or a
   filer-specific extension (the filer's own CIK-based namespace). The leading
   segment before '/' is the taxonomy; extensions won't match the registry."
  [version]
  (when version (first (str/split version #"/"))))

(defn- read-tsv
  "Lazily read a TSV reader into maps keyed by header. Tab-separated, no quoting
   (EDGAR values are unquoted; commas appear literally in text fields)."
  [reader]
  (let [[header & rows] (csv/read-csv reader :separator \tab)
        ks (mapv keyword header)]
    (map #(zipmap ks %) rows)))

(defn- submissions
  "adsh → filing fields, from sub.txt."
  [sub-reader]
  (->> (read-tsv sub-reader)
       (reduce (fn [acc {:keys [adsh cik name form period fy fp filed]}]
                 (assoc acc adsh
                        {:cik (parse/cik->padded cik)
                         :entity-name name
                         :form form
                         :fiscal-year (some-> fy not-empty Long/parseLong)
                         :fiscal-period fp
                         :period period
                         :filed (parse-tsv-date filed)}))
               {})))

(defn- num-row->observation
  "A num.txt row joined to its filing → a normalised observation map (the shape
   the shared pivot consumes), or nil if unregistered, non-consolidated, or
   missing the dates it needs."
  [subs row]
  (let [{:keys [adsh tag version ddate qtrs segments coreg value]} row
        taxonomy (version->taxonomy version)
        reg (get parse/statement-registry [taxonomy tag])]
    (when (and reg
               (str/blank? segments)        ; consolidated only, not a breakdown
               (str/blank? coreg))
      (when-let [{:keys [cik entity-name filed form fiscal-year fiscal-period]} (get subs adsh)]
        (let [period-end (parse-tsv-date ddate)
              n-qtrs (some-> qtrs not-empty Long/parseLong)
              {:keys [statement period]} reg
              ;; duration: derive start = end - qtrs*3 months; instant: no start.
              period-start (when (and (= period :duration) period-end n-qtrs (pos? n-qtrs))
                             (.minusMonths period-end (* 3 n-qtrs)))]
          (when (and period-end filed
                     (or (= period :instant) period-start))
            ;; entity-name rides on the observation so a snapshot can reconstruct
            ;; the issuer without re-reading sub.txt.
            {:cik cik
             :entity-name entity-name
             :statement statement
             :period period
             :column (keyword (parse/snake-case tag))
             :period-start period-start
             :period-end period-end
             :filed filed
             :accession adsh
             :form form
             :fiscal-year fiscal-year
             :fiscal-period fiscal-period
             :value (parse/->decimal value)}))))))

(defn quarter->observations
  "Read one quarter's sub.txt + num.txt readers → the normalised observation maps
   (registry-filtered, consolidated, sub fields joined on); `parse/observations->docs`
   pivots them into statement + issuer docs. Realised eagerly — callers read inside
   with-open."
  [sub-reader num-reader]
  (let [subs (submissions sub-reader)]
    (into [] (keep #(num-row->observation subs %)) (read-tsv num-reader))))

(defn quarter->docs
  "Read one quarter's TSVs → statement + issuer docs for every filer."
  [sub-reader num-reader]
  (parse/observations->docs (quarter->observations sub-reader num-reader)))

;;; Snapshot — the pre-filtered observation stream, written once as gzipped
;;; transit-json so the demo can fetch a small curated artifact (via
;;; download-dataset.sh from S3) instead of re-downloading/parsing ~300MB of raw
;;; TSV. Observations are the post-filter, post-join form; reading back is just
;;; transit-read → parse/observations->docs. XTDB's serde handlers round-trip the
;;; LocalDate / BigDecimal values as idiomatic Clojure (#xt/date etc.).

(defn write-snapshot!
  "Parse a raw quarter (sub/num gz files) and write its observations to `out-file`
   as gzipped transit-json. Returns the observation count."
  [sub-file num-file out-file]
  (io/make-parents out-file)
  (with-open [sub-rdr (parse/gz-reader sub-file)
              num-rdr (parse/gz-reader num-file)
              os (GZIPOutputStream. (io/output-stream out-file))]
    (let [obs (quarter->observations sub-rdr num-rdr)]
      (.write os (serde/write-transit-seq obs :json))
      (count obs))))

(defn read-snapshot
  "Read a gzipped transit-json observation snapshot back into observation maps."
  [in-file]
  (with-open [is (GZIPInputStream. (io/input-stream in-file))]
    ;; the seq is lazy over `is`, which closes with this scope — realise eagerly.
    (vec (serde/read-transit-seq is :json))))

(defn snapshot->docs
  "Read a snapshot and pivot into the same statement + issuer docs a live parse
   yields."
  [in-file]
  (parse/observations->docs (read-snapshot in-file)))

(comment
  ;; Build snapshots from the downloaded quarters, then upload them to S3:
  (doseq [q ["2025q2" "2025q3" "2025q4" "2026q1"]]
    (let [d (str "src/dev/resources/data/edgar/tsv/" q)]
      (println q (write-snapshot! (str d "/sub.txt.gz") (str d "/num.txt.gz")
                                  (str "/tmp/edgar-snapshot/" q ".transit.gz"))))))
