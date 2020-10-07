(ns crux.cache.second-chance
  (:import crux.cache.ICache
           java.lang.reflect.Field
           java.util.function.Function
           [java.util Map$Entry Queue]
           [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap])
  (:require [crux.system :as sys]))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^Field concurrent-map-table-field
  (doto (.getDeclaredField ConcurrentHashMap "table")
    (.setAccessible true)))

(defn- random-entry ^java.util.Map$Entry [^ConcurrentHashMap m]
  (when-not (.isEmpty m)
    (let [table ^objects (.get concurrent-map-table-field m)]
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
    (let [^objects vp (or (.get hot k)
                          (let [k (stored-key-fn k)
                                v (f k)]
                            (.computeIfAbsent hot k (reify Function
                                                      (apply [_ k]
                                                        (doto (object-array 2)
                                                          (aset 0 v)))))))]
      (resize-cache this)
      (aset vp 1 nil)
      (aget vp 0)))

  (evict [_ k]
    (when-let [vp ^objects (.remove hot k)]
      (aset vp 1 nil)))

  (valAt [_ k]
    (when-let [vp ^objects (.get hot k)]
      (aset vp 1 nil)
      (aget vp 0)))

  (valAt [_ k default]
    (let [vp (.getOrDefault hot k default)]
      (if (= default vp)
        default
        (do (aset ^objects vp 1 nil)
            (aget ^objects vp 0)))))

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
        (when-let [vp ^objects (.getValue e)]
          (when (nil? (aget vp 1))
            (when (.offer cold vp)
              (aset vp 1 (.getKey e)))))))))

(defn- resize-cache [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cold ^Queue (.cold cache)]
    (move-to-cold cache)
    (while (> (.size hot) (.size cache))
      (when-let [vp ^objects (.poll cold)]
        (when-let [k (aget vp 1)]
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
