(ns ^:no-doc xtdb.cache.second-chance
  (:import xtdb.cache.ICache
           [xtdb.cache.second_chance ConcurrentHashMapTableAccess ValuePointer]
           java.util.function.Function
           [java.util Map$Entry Queue]
           [java.util.concurrent ConcurrentHashMap LinkedBlockingQueue Semaphore ThreadLocalRandom]
           java.util.concurrent.atomic.AtomicReference)
  (:require [xtdb.system :as sys]
            [xtdb.cache.nop]))

;; Adapted from https://db.in.tum.de/~leis/papers/leanstore.pdf

(set! *unchecked-math* :warn-on-boxed)

(deftype KeyWrapper [k]
  Object
  (equals [_ o] (.equals k (.k ^KeyWrapper o)))
  (hashCode [_] (unchecked-multiply-int 31 (Integer/rotateLeft (.hashCode k) 1))))

(defn- random-entry ^java.util.Map$Entry [^ConcurrentHashMap m]
  (when-not (.isEmpty m)
    (when-let [table (ConcurrentHashMapTableAccess/getConcurrentHashMapTable m)]
      (let [len (alength table)
            start (.nextInt (ThreadLocalRandom/current) len)]
        (loop [i start
               wrapped-around false]
          (if-let [^Map$Entry e (aget table i)]
            e
            (let [next (inc i)
                  last (= next len)
                  next (if last
                         0
                         next)]
              (when-not (and (= next start) wrapped-around)
                (recur next (if last true wrapped-around))))))))))

(declare resize-cache)

(definterface SecondChanceCachePrivate
  (^java.util.Map getHot [])
  (^void maybeResizeCache []))

(deftype SecondChanceCache [^:unsynchronized-mutable ^ConcurrentHashMap hot ^Queue cooling ^double cooling-factor ^ICache cold
                            ^long size adaptive-sizing? ^double adaptive-break-even-level ^double downsize-load-factor
                            ^Semaphore resize-semaphore]
  Object
  (toString [_]
    (str hot))

  SecondChanceCachePrivate
  (getHot [this]
    hot)

  (maybeResizeCache [this]
    (when-not (.isEmpty hot)
      (when-let [table (ConcurrentHashMapTableAccess/getConcurrentHashMapTable hot)]
        (when (< (/ (double (.size hot)) (double (alength table))) downsize-load-factor)
          (set! (.hot this) (doto (ConcurrentHashMap. 0)
                              (.putAll hot)))))))

  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (let [^ValuePointer vp (or (.get hot (KeyWrapper. k))
                               (let [stored-key (stored-key-fn k)
                                     wrapped-key (KeyWrapper. stored-key)
                                     v (if-some [v (.valAt cold wrapped-key)]
                                         v
                                         (f stored-key))
                                     vp (.computeIfAbsent hot wrapped-key (reify Function
                                                                            (apply [_ k]
                                                                              (ValuePointer. v))))]
                                 (resize-cache this)
                                 vp))
          v (.swizzle vp)]
      v))

  (evict [this k]
    (let [k (KeyWrapper. k)]
      (.evict cold k)
      (when-let [vp ^ValuePointer (.remove hot k)]
        (.swizzle vp)))
    (resize-cache this))

  (valAt [_ k]
    (when-let [vp ^ValuePointer (.get hot (KeyWrapper. k))]
      (.swizzle vp)))

  (valAt [_ k default]
    (if-let [vp (.get hot (KeyWrapper. k))]
      (.swizzle ^ValuePointer vp)
      default))

  (count [_]
    (.size hot))

  (close [_]
    (.clear hot)
    (.clear cooling)))

(defn move-to-cooling-state [^SecondChanceCache cache]
  (.maybeResizeCache cache)
  (let [hot ^ConcurrentHashMap (.getHot cache)
        cooling ^Queue (.cooling cache)]
    (while (< (.size cooling)
              (long (Math/ceil (* (.cooling-factor cache) (.size hot)))))
      (when-let [e (random-entry hot)]
        (when-let [vp ^ValuePointer (.getValue e)]
          (when (and (.isSwizzled vp)
                     (.offer cooling vp))
            (.unswizzle vp (.getKey e))))))))

(def ^:private ^AtomicReference free-memory-ratio (AtomicReference. 1.0))
(def ^:private ^:const free-memory-check-interval-ms 1000)

(defn- free-memory-loop []
  (try
    (while true
      (let [max-memory (.maxMemory (Runtime/getRuntime))
            used-memory (- (.totalMemory (Runtime/getRuntime))
                           (.freeMemory (Runtime/getRuntime)))
            free-memory (- max-memory used-memory)]
        (.set free-memory-ratio (double (/ free-memory max-memory))))
      (Thread/sleep free-memory-check-interval-ms))
    (catch InterruptedException _)))

(def ^:private ^Thread free-memory-thread
  (do (when (and (bound? #'free-memory-thread) free-memory-thread)
        (.interrupt ^Thread free-memory-thread))
      (doto (Thread. ^Runnable free-memory-loop "xtdb.cache.second-chance.free-memory-thread")
        (.setDaemon true))))

(defn- move-to-cold-state [^SecondChanceCache cache]
  (let [cooling ^Queue (.cooling cache)
        cold ^ICache (.cold cache)
        hot-target-size (if (.adaptive-sizing? cache)
                          (long (Math/pow (.size cache)
                                          (+ (.adaptive-break-even-level cache)
                                             (double (.get free-memory-ratio)))))
                          (.size cache))]
    (while (> (.size (.getHot cache)) hot-target-size)
      (when-let [vp ^ValuePointer (.poll cooling)]
        (when-some [k (.getKey vp)]
          (.remove (.getHot cache) k)
          (.computeIfAbsent cold k identity (constantly (.getValue vp)))))
      (move-to-cooling-state cache))))

(defn- resize-cache [^SecondChanceCache cache]
  (let [resize-semaphore ^Semaphore (.resize-semaphore cache)]
    (when (.tryAcquire resize-semaphore)
      (try
        (move-to-cooling-state cache)
        (move-to-cold-state cache)
        (finally
          (.release resize-semaphore))))))

(defn ->second-chance-cache
  {::sys/deps {:cold-cache 'xtdb.cache.nop/->nop-cache}
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
                                           :default 0.8
                                           :spec ::sys/pos-double}
               :downsize-load-factor {:doc "Downsize load factor"
                                      :default 0.01
                                      :spec ::sys/pos-double}}}
  ^xtdb.cache.ICache [{:keys [^long cache-size ^double cooling-factor cold-cache
                              adaptive-sizing? adaptive-break-even-level downsize-load-factor]
                       :or {cache-size  (* 128 1024)
                            adaptive-sizing? true
                            cooling-factor 0.1
                            adaptive-break-even-level 0.8
                            downsize-load-factor 0.01}
                       :as opts}]
  (let [hot (ConcurrentHashMap. 0)
        cooling (LinkedBlockingQueue.)
        cold (or cold-cache (xtdb.cache.nop/->nop-cache opts))]
    (when adaptive-sizing?
      (locking free-memory-thread
        (when-not (.isAlive free-memory-thread)
          (.start free-memory-thread))))
    (->SecondChanceCache hot cooling cooling-factor cold cache-size adaptive-sizing? adaptive-break-even-level downsize-load-factor (Semaphore. 1))))
