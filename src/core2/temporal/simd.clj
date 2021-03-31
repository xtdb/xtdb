(ns ^{:clojure.tools.namespace.repl/load false,
      :clojure.tools.namespace.repl/unload false}
    core2.temporal.simd
  (:import jdk.incubator.vector.VectorOperators
           jdk.incubator.vector.LongVector))

;; This is almost an order of magnitude slower than a plain loop,
;; might either be the use case or the API not being used properly.

(defmacro in-range? [min-range point max-range]
  `(let [len# (alength ~point)]
     (loop [n# (int 0)]
       (if (< n# len#)
         (let [mask# (.indexInRange LongVector/SPECIES_PREFERRED n# len#)
               min-range-v# (LongVector/fromArray LongVector/SPECIES_PREFERRED ~min-range n# mask#)
               max-range-v# (LongVector/fromArray LongVector/SPECIES_PREFERRED ~max-range n# mask#)
               point-v# (LongVector/fromArray LongVector/SPECIES_PREFERRED ~point n# mask#)]
           (if (= (.trueCount mask#)
                  (.trueCount (.and (.compare min-range-v# VectorOperators/LE point-v# mask#)
                                    (.compare point-v# VectorOperators/LE max-range-v# mask#))))
               (recur (+ n# (.length LongVector/SPECIES_PREFERRED)))
               false))
         true))))

(defmacro ^:private in-range-column? [min-range point-vec coordinates-vec idx max-range]
  `(let [k# (alength ~min-range)
         point# (long-array k#)
         element-start-idx# (.getElementStartIndex ~point-vec ~idx)]
     (dotimes [n# k#]
       (aset point# n# (.get ~coordinates-vec (+ element-start-idx# n#))))
     (in-range? ~min-range point# ~max-range)))

;; Disabled for now, see above

#_(doseq [v [#'in-range? #'in-range-column?]]
    (intern (find-ns 'core2.temporal.kd-tree) (with-meta (:name (meta v)) {:macro true}) v))
