(ns xtdb.bitemporal
  (:require [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [org.apache.arrow.memory.util ArrowBufPointer]
           (xtdb.bitemporal Ceiling Polygon)
           (xtdb.trie EventRowPointer)
           (xtdb.util TemporalBounds)))

(defn- duplicate-ptr [^ArrowBufPointer dst, ^ArrowBufPointer src]
  (.set dst (.getBuf src) (.getOffset src) (.getLength src)))

(defn polygon-calculator
  (^xtdb.bitemporal.Polygon [] (polygon-calculator nil))

  (^xtdb.bitemporal.Polygon [^TemporalBounds temporal-bounds]
   (let [skip-iid-ptr (ArrowBufPointer.)
         prev-iid-ptr (ArrowBufPointer.)
         current-iid-ptr (ArrowBufPointer.)

         ceiling (Ceiling.)
         polygon (Polygon.)]

     (fn calculate-polygon [^EventRowPointer ev-ptr]
       (when-not (.equals skip-iid-ptr (.getIidPointer ev-ptr current-iid-ptr))
         (when-not (.equals prev-iid-ptr current-iid-ptr)
           (.reset ceiling)
           (duplicate-ptr prev-iid-ptr current-iid-ptr))

         (let [leg (.getOp ev-ptr)]
           (if (.equals "erase" leg)
             (do
               (.reset ceiling)
               (duplicate-ptr skip-iid-ptr current-iid-ptr)
               nil)

             (let [system-from (.getSystemFrom ev-ptr)
                   valid-from (.getValidFrom ev-ptr)
                   valid-to (.getValidTo ev-ptr)]
               (when (or (nil? temporal-bounds)
                         (< system-from (.getUpper (.getSystemTime temporal-bounds))))
                 (.calculateFor polygon ceiling valid-from valid-to)
                 (.applyLog ceiling system-from valid-from valid-to)
                 polygon)))))))))
