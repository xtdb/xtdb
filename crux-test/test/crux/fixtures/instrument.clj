(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [clojure.tools.logging :as log]))

(defn- i-name [i]
  (-> i ^java.lang.Class type .getName symbol))

(defrecord InstrumentedLayeredIndex [i]
  db/Index
  (seek-values [this k]
    (println (format "%s - seek" (i-name i)))
    (db/seek-values i k))

  (next-values [this]
    (println (format "%s - next" (i-name i)))
    (db/next-values i))

  db/LayeredIndex
  (open-level [this]
    (println (format "%s - open" (i-name i)))
    (db/open-level i))

  (close-level [this]
    (println (format "%s - close" (i-name i)))
    (db/close-level i))

  (max-depth [this]
    (db/max-depth i)))

(defprotocol Instrument
  (instrument [q]))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [^crux.index.NAryConstrainingLayeredVirtualIndex this]
    (let [this (update this :n-ary-index instrument)]
      (InstrumentedLayeredIndex. this)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [^crux.index.NAryConstrainingLayeredVirtualIndex this]
    (let [this (update this :unary-join-indexes (fn [indexes] (map instrument indexes)))]
      (InstrumentedLayeredIndex. this)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [^crux.index.UnaryJoinVirtualIndex this]
    (let [this (update this :indexes (fn [indexes] (map instrument indexes)))]
      (InstrumentedLayeredIndex. this)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (map instrument (.indexes state))]
      (set! (.indexes state) [lhs rhs])

      (reify crux.index.BinaryJoinLayeredVirtualIndex)
      this))

  Object
  (instrument [this]
    (InstrumentedLayeredIndex. this)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx)))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
