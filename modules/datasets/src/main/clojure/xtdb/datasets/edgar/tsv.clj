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
            [xtdb.datasets.edgar.parse :as parse])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter DateTimeParseException]))

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
      (when-let [{:keys [cik filed form fiscal-year fiscal-period]} (get subs adsh)]
        (let [period-end (parse-tsv-date ddate)
              n-qtrs (some-> qtrs not-empty Long/parseLong)
              {:keys [statement period]} reg
              ;; duration: derive start = end - qtrs*3 months; instant: no start.
              period-start (when (and (= period :duration) period-end n-qtrs (pos? n-qtrs))
                             (.minusMonths period-end (* 3 n-qtrs)))]
          (when (and period-end filed
                     (or (= period :instant) period-start))
            {:cik cik
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

(defn quarter->docs
  "Read one quarter's sub.txt + num.txt readers → statement + issuer docs for
   every filer. Realised eagerly: callers read inside with-open.

   We group-by cik rather than partition-by: SEC documents num.txt's unique key
   but no row ordering, and in practice a given adsh's rows are not contiguous —
   so partition-by would fragment a filer's facts across partitions and pivot
   them into broken statement rows. group-by is safe; the concept filter first
   cuts num.txt to ~10% (the registered tags), keeping the grouped set modest."
  [sub-reader num-reader]
  (let [subs (submissions sub-reader)
        by-cik (->> (read-tsv num-reader)
                    (keep #(num-row->observation subs %))
                    (group-by :cik))]
    (into []
          (mapcat (fn [[cik cik-obs]]
                    ;; name from the earliest filing (deterministic), not an
                    ;; arbitrary submission — keeps issuer name + valid-from from
                    ;; the same filing.
                    (let [earliest (->> cik-obs (sort-by :filed) first)
                          entity-name (get-in subs [(:accession earliest) :entity-name])]
                      (cons (parse/->issuer-doc cik entity-name earliest)
                            (parse/observations->statements cik cik-obs)))))
          by-cik)))
