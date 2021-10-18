(ns core2.edn
  (:require [clojure.instant :as inst])
  (:import java.io.Writer
           [java.time Duration Instant OffsetDateTime ZoneOffset]))

(defn duration-reader [s] (Duration/parse s))

(def ^:private ->odt
  (inst/validated
   (fn [year month day hour min sec nano offset-sign offset-hrs offset-mins]
     (OffsetDateTime/of year month day hour min sec nano
                        (ZoneOffset/ofHoursMinutes (* offset-hrs offset-sign) offset-mins)))))

(defn instant-reader [s]
  (-> ^OffsetDateTime (inst/parse-timestamp ->odt s)
      (.toInstant)))

(when-not (System/getenv "CORE2_DISABLE_EDN_PRINT_METHODS")
  (defmethod print-dup Duration [^Duration d, ^Writer w]
    (.write w (format "#c2/duration \"%s\"" d)))

  (defmethod print-method Duration [d w]
    (print-dup d w))

  (defmethod print-dup Instant [^Instant i, ^Writer w]
    (.write w (format "#c2/instant \"%s\"" i)))

  (defmethod print-method Instant [i w]
    (print-dup i w)))
