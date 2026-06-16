(ns xtdb.datasets.edgar-snapshot-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.datasets.edgar.tsv :as tsv]))

;; A snapshot is the pre-filtered observation stream as gzipped transit-json.
;; Round-tripping it must yield the same observations — and the same docs — as
;; reading the raw quarter, with LocalDate / BigDecimal values intact.

(defn- sample [path] (io/resource (str "edgar/sample/tsv/" path)))

(defn- live-observations []
  (with-open [sub (parse/gz-reader (io/file (sample "sub.txt.gz")))
              num (parse/gz-reader (io/file (sample "num.txt.gz")))]
    (vec (tsv/quarter->observations sub num))))

(defn- comparable [docs]
  (->> docs (map (fn [d] [(:table (meta d)) (into (sorted-map) d)])) set))

(t/deftest test-snapshot-round-trip
  (let [out (io/file "target/edgar-snapshot-test/q.transit.gz")
        n (tsv/write-snapshot! (io/file (sample "sub.txt.gz"))
                               (io/file (sample "num.txt.gz"))
                               out)
        read-back (tsv/read-snapshot out)
        live (live-observations)]
    (t/is (= n (count live)) "writes one row per observation")
    (t/is (= (set live) (set read-back))
          "observations round-trip identically through transit (incl. dates/decimals)")
    (t/is (= (comparable (parse/observations->docs live))
             (comparable (tsv/snapshot->docs out)))
          "docs pivoted from the snapshot match docs from the raw quarter")))
