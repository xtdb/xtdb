(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [crux.memory :as mem]
            [clojure.tools.logging :as log]))

(defn- i-name [i]
  (-> i ^java.lang.Class type .getName (clojure.string/replace #"crux\.index\." "") symbol))

(defn- trunc
  [s n]
  (subs s 0 (min (count s) n)))

(defn- trace-op [foo op depth i extra]
  (println (format "%s %s%s %s %s" (name op) @foo (apply str (take depth (repeat " ")))
                   (i-name i)
                   (if extra extra ""))))

(defn- v->str [v]
  (str (trunc (str (mem/buffer->hex (first v))) 10) " -> " (trunc (str (second v)) 40)))

(defrecord InstrumentedLayeredIndex [i id depth foo]
  db/Index
  (seek-values [this k]
    (trace-op foo :seek depth i id)
    (let [v (db/seek-values i k)]
      (trace-op foo :seek depth i (v->str v))
      v))

  (next-values [this]
    (trace-op foo :next depth i id)
    (db/next-values i))

  db/LayeredIndex
  (open-level [this]
    (trace-op foo :open depth i id)
    (db/open-level i)
    (swap! foo inc))

  (close-level [this]
    (trace-op foo :close depth i id)
    (db/close-level i)
    (swap! foo dec))

  (max-depth [this]
    (db/max-depth i)))

(defprotocol Instrument
  (instrument [i id depth]))

(defn inst [id depth i]
  (instrument i id depth))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this id depth]
    (let [this (update this :n-ary-index (partial inst nil (inc depth)))]
      (InstrumentedLayeredIndex. this id depth (atom 0))))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this id depth]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst nil (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this id depth (atom 0))))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this id depth]
    (let [this (update this :indexes (fn [indexes] (map (partial inst nil (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this id depth (atom 0))))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this id depth]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          id (let [{:keys [e a v]} (-> this meta :clause)]
               (format "[%s %s %s]" e a v))
          [lhs rhs] (map (partial inst id (inc depth)) (.indexes state))]
      (set! (.indexes state) [lhs rhs])
      this))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this id depth]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst nil (inc depth)) (.indexes state)))
      this))

  Object
  (instrument [this id depth]
    (if (instance? InstrumentedLayeredIndex this)
      this
      (InstrumentedLayeredIndex. this id depth (atom 0)))))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx nil 0)))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
