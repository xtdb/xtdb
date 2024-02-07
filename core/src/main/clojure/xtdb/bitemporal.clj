(ns xtdb.bitemporal
  (:require xtdb.indexer.live-index
            xtdb.object-store
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [org.apache.arrow.memory.util ArrowBufPointer]
           (xtdb.bitemporal Ceiling Polygon)
           (xtdb.trie EventRowPointer)
           (xtdb.util TemporalBounds)))

(defn- duplicate-ptr [^ArrowBufPointer dst, ^ArrowBufPointer src]
  (.set dst (.getBuf src) (.getOffset src) (.getLength src)))

(defn polygon-calculator ^xtdb.bitemporal.Polygon [^TemporalBounds temporal-bounds]
  (let [skip-iid-ptr (ArrowBufPointer.)
        prev-iid-ptr (ArrowBufPointer.)
        current-iid-ptr (ArrowBufPointer.)

        ceiling (Ceiling.)
        polygon (Polygon.)]

    (fn calculate-polygon [^EventRowPointer ev-ptr]
      (when-not (= skip-iid-ptr (.getIidPointer ev-ptr current-iid-ptr))
        (when-not (= prev-iid-ptr current-iid-ptr)
          (.reset ceiling)
          (duplicate-ptr prev-iid-ptr current-iid-ptr))

        (let [leg (.getOp ev-ptr)]
          (if (= :erase leg)
            (do
              (.reset ceiling)
              (duplicate-ptr skip-iid-ptr current-iid-ptr)
              nil)

            (let [system-from (.getSystemFrom ev-ptr)]
              (when (.inRange (.getSystemFrom temporal-bounds) system-from)
                (.calculateFor polygon ceiling ev-ptr)
                (.applyLog ceiling system-from
                           (.getValidFrom ev-ptr 0) (.getValidTo ev-ptr (dec (.getValidTimeRangeCount ev-ptr))))
                polygon))))))))
