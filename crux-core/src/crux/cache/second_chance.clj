(ns ^:no-doc crux.cache.second-chance
  (:import crux.cache.ICache
           [crux.cache.second_chance ConcurrentHashMapTableAccess ValuePointer]
           java.util.function.Function
           [java.util Map$Entry Queue]
           [java.util.concurrent ConcurrentHashMap LinkedBlockingQueue]
           java.util.concurrent.atomic.AtomicReference)
  (:require [crux.system :as sys]
            [crux.cache.nop]))

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

(deftype SecondChanceCache [^ConcurrentHashMap hot ^Queue cooling ^double cooling-factor ^ICache cold
                            ^long size adaptive-sizing? ^double adaptive-break-even-level]
  Object
  (toString [_]
    (str hot))

  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (let [^ValuePointer vp (or (.get hot k)
                               (let [k (stored-key-fn k)
                                     v (if-some [v (.valAt cold k)]
                                         v
                                         (f k))]
                                 (.computeIfAbsent hot k (reify Function
                                                           (apply [_ k]
                                                             (ValuePointer. v))))))
          v (.swizzle vp)]
      (resize-cache this)
      v))

  (evict [this k]
    (.evict cold k)
    (when-let [vp ^ValuePointer (.remove hot k)]
      (.swizzle vp))
    (resize-cache this))

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

(def ^:private ^AtomicReference free-memory-ratio (AtomicReference. 1.0))

(defn- free-memory-loop []
  (try
    (while true
      (.set free-memory-ratio (double (/ (.freeMemory (Runtime/getRuntime))
                                         (.totalMemory (Runtime/getRuntime)))))
      (Thread/sleep 5000))
    (catch InterruptedException _)))

(def ^:private ^Thread free-memory-thread
  (do (when (and (bound? #'free-memory-thread) free-memory-thread)
        (.interrupt ^Thread free-memory-thread))
      (doto (Thread. ^Runnable free-memory-loop "crux.cache.second-chance.free-memory-thread")
        (.setDaemon true))))

(defn- move-to-cold-state [^SecondChanceCache cache]
  (let [hot ^ConcurrentHashMap (.hot cache)
        cooling ^Queue (.cooling cache)
        cold ^ICache (.cold cache)
        hot-target-size (if (.adaptive-sizing? cache)
                          (long (Math/pow (.size cache)
                                          (+ (.adaptive-break-even-level cache)
                                             (double (.get free-memory-ratio)))))
                          (.size cache))]
    (while (> (.size hot) hot-target-size)
      (when-let [vp ^ValuePointer (.poll cooling)]
        (when-some [k (.getKey vp)]
          (.remove hot k)
          (.computeIfAbsent cold k identity (constantly (.getValue vp)))))
      (move-to-cooling-state cache))))

(defn- resize-cache [^SecondChanceCache cache]
  (move-to-cooling-state cache)
  (move-to-cold-state cache))

(defn ->second-chance-cache
  {::sys/deps {:cold-cache 'crux.cache.nop/->nop-cache}
   ::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}
               :cooling-factor {:doc "Cooling factor"
                                :default 0.1
                                :spec ::sys/pos-double}
               :adaptive-sizing? {:doc "Adapt cache size to available memory"
                                  :default true
                                  :spec ::sys/boolean}
               :adaptive-break-even-level {:doc "Adaptive break even memory usage level"
                                           :default 0.95
                                           :spec ::sys/pos-double}}}
  ^crux.cache.ICache [{:keys [^long cache-size ^double cooling-factor cold-cache
                              adaptive-sizing? adaptive-break-even-level]
                       :or {cache-size  (* 128 1024)
                            adaptive-sizing? true
                            cooling-factor 0.1
                            adaptive-break-even-level 0.95}
                       :as opts}]
  (let [hot (ConcurrentHashMap. cache-size)
        cooling (LinkedBlockingQueue.)
        cold (or cold-cache (crux.cache.nop/->nop-cache opts))]
    (when adaptive-sizing?
      (locking free-memory-thread
        (when-not (.isAlive free-memory-thread)
          (.start free-memory-thread))))
    (->SecondChanceCache hot cooling cooling-factor cold cache-size adaptive-sizing? adaptive-break-even-level)))
