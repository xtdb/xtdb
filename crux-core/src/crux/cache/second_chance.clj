(ns crux.cache.second-chance
  (:import crux.cache.ICache
           crux.cache.second_chance.ValuePointer
           java.util.function.Function
           [java.util Map$Entry Queue]
           [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap])
  (:require [crux.system :as sys]))

(set! *unchecked-math* :warn-on-boxed)

(defn- random-entry ^java.util.Map$Entry [^ConcurrentHashMap m]
  (when-not (.isEmpty m)
    (let [table (ValuePointer/getConcurrentHashMapTable m)]
      (loop [i (long (rand-int (alength table)))]
        (if-let [^Map$Entry e (aget table i)]
          e
          (recur (rem (inc i) (alength table))))))))

(declare resize-cache)

(deftype SecondChanceCache [^ConcurrentHashMap hot ^Queue cold ^double cold-factor ^long size]
  Object
  (toString [_]
    (str hot " " cold))

  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (let [^ValuePointer vp (or (.get hot k)
                               (let [k (stored-key-fn k)
                                     v (f k)
                                     vp (.computeIfAbsent hot k (reify Function
                                                                  (apply [_ k]
                                                                    (ValuePointer. v))))]
                                 (resize-cache this)
                                 vp))]
      (set! (.coldKey vp) nil)
      (.value vp)))

  (evict [_ k]
    (when-let [vp ^ValuePointer (.remove hot k)]
      (set! (.coldKey vp) nil)))

  (valAt [_ k]
    (when-let [vp ^ValuePointer (.get hot k)]
      (set! (.coldKey vp) nil)
      (.value vp)))

  (valAt [_ k default]
    (let [vp (.getOrDefault hot k default)]
      (if (= default vp)
        default
        (let [vp ^ValuePointer vp]
          (set! (.coldKey vp) nil)
          (.value vp)))))

  (count [_]
    (.size hot))

  (close [_]
    (.clear hot)
    (.clear cold)))

(defn move-to-cold [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cold ^Queue (.cold cache)
        cold-target-size (long (Math/ceil (* (.cold-factor cache) (.size hot))))]
    (while (< (.size cold) cold-target-size)
      (let [e (random-entry (.hot cache))]
        (when-let [vp ^ValuePointer (.getValue e)]
          (when (nil? (.coldKey vp))
            (when (.offer cold vp)
              (set! (.coldKey vp) (.getKey e)))))))))

(defn- resize-cache [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cold ^Queue (.cold cache)]
    (move-to-cold cache)
    (while (> (.size hot) (.size cache))
      (when-let [vp ^ValuePointer (.poll cold)]
        (when-some [k (.coldKey vp)]
          (.remove hot k)))
      (move-to-cold cache))))

(defn ->second-chance-cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}
               :cold-factor {:doc "Cold factor"
                             :default 0.1
                             :spec ::sys/pos-double}}}
  [{:keys [^long cache-size ^double cold-factor]}]
  (let [hot (ConcurrentHashMap. cache-size)
        cold-factor (or cold-factor 0.1)
        cold (ArrayBlockingQueue. (inc (long (Math/ceil (* cold-factor cache-size)))))]
    (->SecondChanceCache hot cold cold-factor cache-size)))
