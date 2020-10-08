(ns crux.cache.second-chance
  (:import crux.cache.ICache
           [crux.cache.second_chance ConcurrentHashMapTableAccess ValuePointer]
           java.util.function.Function
           [java.util Map$Entry Queue]
           [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap])
  (:require [crux.system :as sys]))

;; Adapted from https://db.in.tum.de/~leis/papers/leanstore.pdf

(set! *unchecked-math* :warn-on-boxed)

(defn- random-entry ^java.util.Map$Entry [^ConcurrentHashMap m]
  (when-not (.isEmpty m)
    (let [table (ConcurrentHashMapTableAccess/getConcurrentHashMapTable m)]
      (loop [i (long (rand-int (alength table)))]
        (if-let [^Map$Entry e (aget table i)]
          e
          (recur (rem (inc i) (alength table))))))))

(declare resize-cache)

(deftype SecondChanceCache [^ConcurrentHashMap hot ^Queue cooling ^double cooling-factor ^long size]
  Object
  (toString [_]
    (str hot " " cooling))

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
      (.swizzle vp)))

  (evict [_ k]
    (when-let [vp ^ValuePointer (.remove hot k)]
      (.swizzle vp)))

  (valAt [_ k]
    (when-let [vp ^ValuePointer (.get hot k)]
      (.swizzle vp)))

  (valAt [_ k default]
    (if-let [vp (.get hot k)]
      (.swizzle ^ValuePointer vp)
      default))

  (count [_]
    (.size hot))

  (close [_]
    (.clear hot)
    (.clear cooling)))

(defn move-to-cooling-state [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cooling ^Queue (.cooling cache)
        cooling-target-size (long (Math/ceil (* (.cooling-factor cache) (.size hot))))]
    (while (< (.size cooling) cooling-target-size)
      (let [e (random-entry (.hot cache))]
        (when-let [vp ^ValuePointer (.getValue e)]
          (when (and (.isSwizzled vp)
                     (.offer cooling vp))
            (.unswizzle vp (.getKey e))))))))

(defn- resize-cache [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cooling ^Queue (.cooling cache)]
    (move-to-cooling-state cache)
    (while (> (.size hot) (.size cache))
      (when-let [vp ^ValuePointer (.poll cooling)]
        (when-some [k (.getKey vp)]
          (.remove hot k)))
      (move-to-cooling-state cache))))

(defn ->second-chance-cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}
               :cooling-factor {:doc "Cooling factor"
                                :default 0.1
                                :spec ::sys/pos-double}}}
  [{:keys [^long cache-size ^double cooling-factor]}]
  (let [hot (ConcurrentHashMap. cache-size)
        cooling-factor (or cooling-factor 0.1)
        cooling (ArrayBlockingQueue. (inc (long (Math/ceil (* cooling-factor cache-size)))))]
    (->SecondChanceCache hot cooling cooling-factor cache-size)))
