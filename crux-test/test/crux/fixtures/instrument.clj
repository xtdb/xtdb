(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [clojure.tools.logging :as log]))

(defn- i-name [i]
  (-> i ^java.lang.Class type .getName (clojure.string/replace #"crux\.index\." "") symbol))

(defn- trace-op [depth i op]
  (println (format "%s%s - %s" (apply str (take depth (repeat "             "))) (i-name i) (name op))))

(defrecord InstrumentedLayeredIndex [i depth]
  db/Index
  (seek-values [this k]
    (trace-op depth i (str "seek-" k))
    (db/seek-values i k))

  (next-values [this]
    (trace-op depth i :next)
    (db/next-values i))

  db/LayeredIndex
  (open-level [this]
    (trace-op depth i :open)
    (db/open-level i))

  (close-level [this]
    (trace-op depth i :close)
    (db/close-level i))

  (max-depth [this]
    (db/max-depth i)))

(defprotocol Instrument
  (instrument [q depth]))

(defn inst [depth i]
  (instrument i depth))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this depth]
    (let [this (update this :n-ary-index (partial inst (inc depth)))]
      (InstrumentedLayeredIndex. this depth)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this depth]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this depth)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this depth]
    (let [this (update this :indexes (fn [indexes] (map (partial inst (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this depth)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this depth]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (map (partial inst (inc depth)) (.indexes state))]
      (set! (.indexes state) [lhs rhs])
      this))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this depth]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst (inc depth)) (.indexes state)))
      this))

  Object
  (instrument [this depth]
    (if (instance? InstrumentedLayeredIndex this)
      this
      (InstrumentedLayeredIndex. this depth))))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx 0)))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
