(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [crux.memory :as mem]
            [clojure.tools.logging :as log]))

(defn- i-name [i]
  (-> i ^java.lang.Class type .getName (clojure.string/replace #"crux\.index\." "") symbol))

(defn- trace-op [id depth i op & [extra]]
  (println (format "%s%s %s%s %s" (name op) (apply str (take depth (repeat " ")))
                   (if id (str id "-") "")
                   (i-name i)
                   (if extra extra ""))))

(defmulti do-seek-values (fn [_ _ i k] (i-name i)))

(defmethod do-seek-values :default [id depth i k]
  (trace-op id depth i :seek)
  (db/seek-values i k))

;; AVE

(defmethod do-seek-values 'DocAttributeValueEntityValueIndex [id depth i k]
  (let [v (db/seek-values i k)]
    (trace-op id depth i :seek (str " ->" (mem/buffer->hex (first v)) "-" (second v)))
    v))

(defmethod do-seek-values 'DocAttributeValueEntityEntityIndex [id depth i k]
  (let [v (db/seek-values i k)]
    (trace-op id depth i :seek (str " ->" (mem/buffer->hex (first v)) "-" (second v)))
    v))

;; AEV

(defmethod do-seek-values 'DocAttributeEntityValueEntityIndex [id depth i k]
  (let [v (db/seek-values i k)]
    (trace-op id depth i :seek (str " ->" (mem/buffer->hex (first v)) "-" (second v)))
    v))

(defrecord InstrumentedLayeredIndex [i id depth]
  db/Index
  (seek-values [this k]
    (do-seek-values id depth i k))

  (next-values [this]
    (trace-op id depth i :next)
    (db/next-values i))

  db/LayeredIndex
  (open-level [this]
    (trace-op id depth i :open)
    (db/open-level i))

  (close-level [this]
    (trace-op id depth i :close)
    (db/close-level i))

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
      (InstrumentedLayeredIndex. this id depth)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this id depth]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst nil (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this id depth)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this id depth]
    (let [this (update this :indexes (fn [indexes] (map (partial inst nil (inc depth)) indexes)))]
      (InstrumentedLayeredIndex. this id depth)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this id depth]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (map (partial inst (-> this meta :clause str) (inc depth)) (.indexes state))]
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
      (InstrumentedLayeredIndex. this id depth))))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx nil 0)))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
