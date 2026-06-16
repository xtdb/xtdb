(ns xtdb.datasets.edgar-snapshot-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.mirror :as mirror]
            [xtdb.datasets.edgar.parse :as parse])
  (:import [java.util.zip GZIPInputStream]))

;; The mirror writes a quarter's curated observation stream as gzipped
;; transit-json. Reading it back must yield the same observations — and the same
;; docs — as reading the raw quarter, with LocalDate / BigDecimal values intact.

(defn- sample [path] (io/resource (str "edgar/sample/tsv/" path)))

(defn- live-observations []
  (with-open [sub (parse/gz-reader (io/file (sample "sub.txt.gz")))
              num (parse/gz-reader (io/file (sample "num.txt.gz")))]
    (vec (mirror/quarter->observations sub num))))

(defn- comparable [docs]
  (->> docs (map (fn [d] [(:table (meta d)) (into (sorted-map) d)])) set))

(t/deftest test-mirror-round-trip
  (let [out-dir (io/file "target/edgar-mirror-test/transit")
        out-file (io/file out-dir "2025q4.transit.json.gz")]
    ;; mirror! fetches from SEC; use its local seam to write transit from the
    ;; committed fixture readers, then read it back.
    (with-open [sub (parse/gz-reader (io/file (sample "sub.txt.gz")))
                num (parse/gz-reader (io/file (sample "num.txt.gz")))]
      (mirror/write-quarter-transit! sub num out-file))
    (let [read-back (with-open [in (-> (io/input-stream out-file) GZIPInputStream.)]
                      (edgar/read-records in))
          live (live-observations)]
      (t/is (= (set live) (set read-back))
            "observations round-trip identically through transit (incl. dates/decimals)")
      (t/is (= (comparable (parse/observations->docs live))
               (comparable (parse/observations->docs read-back)))
            "docs pivoted from the mirrored transit match docs from the raw quarter"))))
