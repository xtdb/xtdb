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
  (format "Binary: [%s %s %s]" (-> i meta :clause :e) (-> i meta :clause :a) (-> i meta :clause :v)))

(defmethod index-name 'crux.index.DocAttributeValueEntityEntityIndex [i]
  "AVE-E:")

(defmethod index-name 'crux.index.DocAttributeValueEntityValueIndex [i]
  "AVE-V:")

(defmethod index-name 'crux.index.DocAttributeEntityValueEntityIndex [i]
  "AVE-E:")

(defmethod index-name 'crux.index.DocAttributeEntityValueValueIndex [i]
  "AEV-V:")

(defn- trunc
  [s n]
  (subs s 0 (min (count s) n)))

(defn- trace-op [foo op depth & extra]
  (print (format "%s %s%s %s" (name op) @foo (apply str (take (get @depth op) (repeat " ")))
                   (clojure.string/join " " extra))))

(defn- v->str [v]
  (str "["(trunc (str (mem/buffer->hex (first v))) 10) " " (trunc (str (second v)) 40) "]"))

(defmulti index-seek (fn [i id depth foo k] (-> i ^java.lang.Class type .getName symbol)))

(defmethod index-seek :default [i id depth foo k]
  (trace-op foo :seek depth (index-name i) id)
  (if (#{'crux.index.DocAttributeValueEntityEntityIndex
         'crux.index.DocAttributeValueEntityValueIndex
         'crux.index.DocAttributeEntityValueEntityIndex
         'crux.index.DocAttributeEntityValueValueIndex}
       (-> i ^java.lang.Class type .getName symbol))
    (do
      (swap! depth update :seek inc)
      (let [v (db/seek-values i k)]
        (println (v->str v))
        (swap! depth update :seek dec)
        v))
    (do
      (println)
      (swap! depth update :seek inc)
      (let [v (db/seek-values i k)]
        (trace-op foo :seek depth "--->" (v->str v))
        (println)
        (swap! depth update :seek dec)
        v))))

(defrecord InstrumentedLayeredIndex [i id depth foo]
  db/Index
  (seek-values [this k]
    (index-seek i id depth foo k))

  (next-values [this]
    (trace-op foo :next depth (index-name i) id)
    (println)
    (swap! depth update :next inc)
    (let [v (db/next-values i)]
      (swap! depth update :next dec)
      v))

  db/LayeredIndex
  (open-level [this]
    (swap! foo inc)
    (db/open-level i))

  (close-level [this]
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
    (let [this (update this :n-ary-index (partial inst nil depth visited))]
      (->instrumented-index this id depth visited)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this id depth visited]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst nil depth visited) indexes)))]
      (->instrumented-index this id depth visited)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this id depth visited]
    (let [this (update this :indexes (fn [indexes] (map (partial inst nil depth visited) indexes)))]
      (->instrumented-index this id depth visited)))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this id depth visited]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (map (partial inst id depth visited) (.indexes state))]
      (set! (.indexes state) [lhs rhs])
      (let [id (let [{:keys [e a v]} (-> this meta :clause)]
                 (format "[%s %s %s]" e a v))
            id (:name this)]
        (merge (->instrumented-index this id depth visited) this))))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this id depth visited]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst nil depth visited) (.indexes state)))
      (->instrumented-index this id depth visited)))

  Object
  (instrument [this id depth visited]
    (->instrumented-index this id depth visited)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx nil (atom {:seek 0 :next 0}) (atom {}))))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
