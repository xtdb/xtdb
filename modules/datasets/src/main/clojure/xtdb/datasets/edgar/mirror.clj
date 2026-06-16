(ns xtdb.datasets.edgar.mirror
  "Produces the EDGAR dataset for S3: SEC EDGAR 'Financial Statement Data Sets' —
   the quarterly TSV bulk dumps — read, joined, curated and written to transit in
   one Clojure pass.

   Run occasionally, from the REPL (see the comment at the foot of this ns): for
   each quarter `mirror!` reads its sub.txt + num.txt, keeps only the
   consolidated rows whose (taxonomy, tag) is registered, and writes the curated
   observation records as <quarter>.transit.json.gz into an out-dir;
   scripts/upload-edgar.sh then syncs that dir to s3://xtdb-datasets/edgar/.
   Consumers sync it *down* via download-dataset.sh --edgar and load with
   xtdb.datasets.edgar — no raw-TSV reading or registry filtering on the load
   path, just transit + the standard serde handlers.

   A quarter ZIP holds, among others:
   - sub.txt — one row per filing: adsh, cik, name, form, period, fy, fp, filed,
     accepted (system-time, to the second).
   - num.txt — one row per fact: adsh, tag, version, ddate (period end), qtrs
     (0 = instant balance, else a duration spanning that many quarters), uom,
     segments, coreg, value.

   We join num→sub on adsh, keep only consolidated rows (blank segments/coreg)
   whose (taxonomy, tag) is registered, derive each fact's period from
   ddate/qtrs, and emit the normalised observation maps the loader's shared pivot
   (xtdb.datasets.edgar.parse) consumes."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.serde :as serde])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [java.util.zip GZIPOutputStream]))

(defn cik->padded
  "EDGAR's canonical CIK is 10-digit zero-padded; sub.txt gives a bare integer.
   Normalise to the padded form the loader and the EdgarPgIndexer key on."
  [cik]
  (format "%010d" (long (if (string? cik) (Long/parseLong (str/trim cik)) cik))))

;; EDGAR TSV dates are yyyymmdd (no separators).
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
                        {:cik (cik->padded cik)
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
            ;; entity-name rides on the observation so the loader can reconstruct
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

(defn- mirror-quarter!
  "Parse a raw quarter (sub/num gz files) and write its curated observations to
   `out-file` as gzipped transit-json. Returns the observation count."
  [sub-file num-file out-file]
  (io/make-parents out-file)
  (with-open [sub-rdr (parse/gz-reader sub-file)
              num-rdr (parse/gz-reader num-file)
              os (GZIPOutputStream. (io/output-stream out-file))]
    (let [obs (quarter->observations sub-rdr num-rdr)]
      (.write os (serde/write-transit-seq obs :json))
      (log/infof "  mirrored %d observations → %s" (count obs) (.getName (io/file out-file)))
      (count obs))))

(defn mirror!
  "Mirror a raw-quarters dir to curated transit under out-dir: each quarter's
   raw sub.txt.gz + num.txt.gz (under `<raw-dir>/<quarter>/`) is read, filtered
   to the registered consolidated observations, and written as
   `<quarter>.transit.json.gz`. Run from the REPL (see the comment below);
   upload-edgar.sh then syncs out-dir → s3://xtdb-datasets/edgar/.

   `quarters` defaults to every immediate sub-dir of raw-dir that carries both
   sub.txt.gz and num.txt.gz."
  ([raw-dir out-dir] (mirror! raw-dir out-dir nil))
  ([raw-dir out-dir {:keys [quarters]}]
   (let [raw (io/file raw-dir)
         quarters (or quarters
                      (->> (.listFiles raw)
                           (filter (fn [^java.io.File d]
                                     (and (.isDirectory d)
                                          (.exists (io/file d "sub.txt.gz"))
                                          (.exists (io/file d "num.txt.gz")))))
                           (map #(.getName ^java.io.File %))
                           sort))]
     (log/info "Mirroring EDGAR quarters" (vec quarters) "→" (str out-dir))
     (doseq [q quarters]
       (mirror-quarter! (io/file raw q "sub.txt.gz")
                        (io/file raw q "num.txt.gz")
                        (io/file out-dir (str q ".transit.json.gz"))))
     (log/info "EDGAR mirror complete →" (str out-dir)))))

(comment
  ;; produce the demo dataset from the downloaded raw quarters
  ;; (scripts/download-edgar-tsv.sh leaves them under .../edgar/tsv/<quarter>/):
  (mirror! "src/dev/resources/data/edgar/tsv" "src/dev/resources/data/edgar")
  ;; → scripts/upload-edgar.sh   (aws s3 sync that dir → s3://xtdb-datasets/edgar/)
  )
