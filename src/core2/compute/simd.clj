(ns ^{:clojure.tools.namespace.repl/load false,
      :clojure.tools.namespace.repl/unload false}
    core2.compute.simd
  (:require [core2.compute :as cc])
  (:import jdk.incubator.vector.VectorOperators
           jdk.incubator.vector.DoubleVector
           jdk.incubator.vector.LongVector))
