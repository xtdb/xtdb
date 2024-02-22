(ns xtdb.walk
  (:import clojure.lang.MapEntry
           [java.util List Map Map$Entry Set]))

(defn walk-java
  "see clojure.walk/walk, but also applies to non-Clojure collections"
  [inner outer form]

  (cond
    (list? form) (outer (apply list (map inner form)))
    (instance? Map$Entry form)
    (outer (MapEntry/create (inner (key form)) (inner (val form))))
    (seq? form) (outer (doall (map inner form)))
    (coll? form) (outer (into (empty form) (map inner) form))
    (instance? List form) (outer (into [] (map inner) form))
    (instance? Map form) (outer (into {} (map inner) form))
    (instance? Set form) (outer (into #{} (map inner) form))
    :else (outer form)))

(defn postwalk-java
  "see clojure.walk/postwalk, but also applies to non-Clojure collections"
  [f form]

  (walk-java (partial postwalk-java f) f form))

(defn update-nested-keys [obj f]
  (->> obj
       (postwalk-java
        (fn [v]
          (cond-> v
            (instance? Map v) (update-keys f))))))

