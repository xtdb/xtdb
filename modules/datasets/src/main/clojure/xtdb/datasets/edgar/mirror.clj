(ns xtdb.datasets.edgar.mirror
  "Produces the EDGAR dataset for S3: SEC EDGAR 'Financial Statement Data Sets' —
   the quarterly TSV bulk dumps — fetched, joined, curated and written to transit
   in one Clojure pass.

   Run occasionally, from the REPL (see the comment at the foot of this ns): for
   each quarter `mirror!` downloads the quarter ZIP from SEC, reads its sub.txt +
   num.txt, keeps only the consolidated rows whose (taxonomy, tag) is registered,
   and writes the curated observation records as <quarter>.transit.json.gz into an
   out-dir; scripts/upload-edgar.sh then syncs that dir to s3://xtdb-datasets/edgar/.
   Consumers sync it *down* via download-dataset.sh --edgar and load with
   xtdb.datasets.edgar — no SEC fetch, raw-TSV reading or registry filtering on
   the load path, just transit + the standard serde handlers.

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
            [hato.client :as http]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.serde :as serde])
  (:import [java.io File]
           [java.time LocalDate]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [java.util.zip GZIPOutputStream ZipFile]))

;; SEC requires a descriptive User-Agent with a contact (a blank one 403s).
(def ^:private user-agent "XTDB datasets hello@xtdb.com")

(def ^:private base-url
  "https://www.sec.gov/files/dera/data/financial-statement-data-sets")

;; The standard demo window — the last ~year of published quarters. Override by
;; passing :quarters to mirror!.
(def default-quarters ["2025q2" "2025q3" "2025q4" "2026q1"])

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

(defn- fetch-quarter-zip!
  "Download a quarter's Financial Statement Data Set ZIP from SEC to a temp file,
   streamed (they're 40–100MB). The caller owns + deletes it."
  ^File [quarter]
  (let [url (str base-url "/" quarter ".zip")
        tmp (File/createTempFile (str "edgar-" quarter "-") ".zip")]
    (log/info "fetching" url)
    (with-open [in (:body (http/get url {:as :stream :headers {"User-Agent" user-agent}}))]
      (io/copy in tmp))
    tmp))

(defn- zip-entry-reader
  "A reader over one named entry in a ZipFile (e.g. \"num.txt\"). The caller owns
   the ZipFile and keeps it open while reading."
  ^java.io.Reader [^ZipFile zf entry-name]
  (io/reader (.getInputStream zf (.getEntry zf entry-name))))

(defn write-quarter-transit!
  "Read one quarter's sub.txt + num.txt (readers), curate to observations, and
   write them to `out-file` as gzipped transit-json. The local-file seam the
   fetch path and the tests share. Returns the observation count."
  [sub-reader num-reader out-file]
  (io/make-parents out-file)
  (with-open [os (GZIPOutputStream. (io/output-stream out-file))]
    (let [obs (quarter->observations sub-reader num-reader)]
      (.write os (serde/write-transit-seq obs :json))
      (log/infof "  mirrored %d observations → %s" (count obs) (.getName (io/file out-file)))
      (count obs))))

(defn- mirror-quarter!
  "Fetch a quarter ZIP from SEC, read its sub.txt + num.txt, and write the curated
   observations to `out-file`. A ZipFile gives random access, so we read the whole
   sub.txt into the filing map before streaming num.txt against it."
  [quarter out-file]
  (let [zip (fetch-quarter-zip! quarter)]
    (try
      (with-open [zf (ZipFile. zip)
                  sub-rdr (zip-entry-reader zf "sub.txt")
                  num-rdr (zip-entry-reader zf "num.txt")]
        (write-quarter-transit! sub-rdr num-rdr out-file))
      (finally (.delete zip)))))

(defn mirror!
  "Fetch + curate EDGAR straight to transit under out-dir: for each quarter,
   download its Financial Statement Data Set ZIP from SEC, filter to the
   registered consolidated observations, and write `<quarter>.transit.json.gz`.
   Run from the REPL (see the comment below); upload-edgar.sh then syncs out-dir →
   s3://xtdb-datasets/edgar/.

   `quarters` defaults to `default-quarters` (the last ~year). Each is a
   yyyy'q'N tag, e.g. \"2025q4\"."
  ([out-dir] (mirror! out-dir nil))
  ([out-dir {:keys [quarters] :or {quarters default-quarters}}]
   (log/info "Mirroring EDGAR quarters" (vec quarters) "→" (str out-dir))
   (doseq [q quarters]
     (when (Thread/interrupted)
       (throw (InterruptedException. "interrupted mirroring EDGAR")))
     (mirror-quarter! q (io/file out-dir (str q ".transit.json.gz"))))
   (log/info "EDGAR mirror complete →" (str out-dir))))

(comment
  ;; produce the demo dataset (the default ~year of quarters), then sync to S3:
  (mirror! "src/dev/resources/data/edgar")
  ;; → scripts/upload-edgar.sh   (aws s3 sync that dir → s3://xtdb-datasets/edgar/)

  ;; a specific window:
  (mirror! "src/dev/resources/data/edgar" {:quarters ["2024q1" "2024q2"]}))
