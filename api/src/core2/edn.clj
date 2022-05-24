(ns ^{:clojure.tools.namespace.repl/load false}
  core2.edn
  (:require [time-literals.read-write :as time-literals]
            [clojure.string :as str])
  (:import java.io.Writer
           [java.time Duration Period]
           [org.apache.arrow.vector PeriodDuration]))

(defn period-duration-reader [pd]
  (let [[p d] (str/split pd #" ")]
    (PeriodDuration. (Period/parse p) (Duration/parse d))))

(defn- print-time-to-string [t o]
  (str "#time/" t " \"" (str o) "\""))

(when-not (System/getenv "CORE2_DISABLE_EDN_PRINT_METHODS")

  (time-literals/print-time-literals-clj!)

  (defmethod print-dup PeriodDuration [c ^Writer w]
    (.write w ^String (print-time-to-string "period-duration" c)))

  (defmethod print-method PeriodDuration [c ^Writer w]
    (print-dup c w)))
