(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [crux.memory :as mem]
            [clojure.tools.logging :as log]))

(defmulti index-name (fn [i] (-> i ^java.lang.Class type .getName symbol)))

(defmethod index-name :default [i]
  (-> i ^java.lang.Class type .getName (clojure.string/replace #"crux\.index\." "") symbol))

(defmethod index-name 'crux.index.NAryConstrainingLayeredVirtualIndex [i]
  (str "NAry (Constrained): " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.NAryJoinLayeredVirtualIndex [i]
  (str "NAry: " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.UnaryJoinVirtualIndex [i]
  (str "Unary: " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.BinaryJoinLayeredVirtualIndex [i]
  (str "Binary: " (:name i)))

(defmethod index-name 'crux.index.DocAttributeValueEntityEntityIndex [i]
  (str "AVE-E: " (:name i)))

(defmethod index-name 'crux.index.DocAttributeValueEntityValueIndex [i]
  (str "AVE-V: " (:name i)))

(defmethod index-name 'crux.index.DocAttributeEntityValueEntityIndex [i]
  (str "AVE-E: " (:name i)))

(defmethod index-name 'crux.index.DocAttributeEntityValueValueIndex [i]
  (str "AEV-V: " (:name i)))

(defn- trunc
  [s n]
  (subs s 0 (min (count s) n)))

(defn- trace-op [foo op depth & extra]
  (println (format "%s %s%s %s" (name op) @foo (apply str (take depth (repeat " ")))
                   (clojure.string/join " " extra))))

(defn- v->str [v]
  (str (trunc (str (mem/buffer->hex (first v))) 10) " -> " (trunc (str (second v)) 40)))

(defrecord InstrumentedLayeredIndex [i id depth foo]
  db/Index
  (seek-values [this k]
    (trace-op foo :seek depth (index-name i) id)
    (let [v (db/seek-values i k)]
      (trace-op foo :seek depth "--->" (v->str v))
      v))

  (next-values [this]
    (trace-op foo :next depth (index-name i) id)
    (db/next-values i))

  db/LayeredIndex
  (open-level [this]
    (swap! foo inc)
;;    (trace-op foo :open depth (index-name i) id)
    (db/open-level i))

  (close-level [this]
;;    (trace-op foo :close depth (index-name i) id)
    (db/close-level i)
    (swap! foo dec))

  (max-depth [this]
    (db/max-depth i)))

(defprotocol Instrument
  (instrument [i id depth visited]))

(defn inst [id depth visited i]
  (instrument i id depth visited))

(defn ->instrumented-index [i id depth visited]
  (or (and (instance? InstrumentedLayeredIndex i) i)
      (get @visited i)
      (let [ii (InstrumentedLayeredIndex. i id depth (atom 0))]
        (swap! visited assoc i ii)
        ii)))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this id depth visited]
    (let [this (update this :n-ary-index (partial inst nil (inc depth) visited))]
      (->instrumented-index this id depth visited)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this id depth visited]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst nil (inc depth) visited) indexes)))]
      (->instrumented-index this id depth visited)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this id depth visited]
    (let [this (update this :indexes (fn [indexes] (map (partial inst nil (inc depth) visited) indexes)))]
      (->instrumented-index this id depth visited)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this id depth visited]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (map (partial inst id (inc depth) visited) (.indexes state))]
      (set! (.indexes state) [lhs rhs])
      (let [id (let [{:keys [e a v]} (-> this meta :clause)]
                 (format "[%s %s %s]" e a v))
            id (:name this)]
        (merge (->instrumented-index this id depth visited) this))))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this id depth visited]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst nil (inc depth) visited) (.indexes state)))
      (->instrumented-index this id depth visited)))

  Object
  (instrument [this id depth visited]
    (->instrumented-index this id depth visited)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx nil 0 (atom {}))))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
