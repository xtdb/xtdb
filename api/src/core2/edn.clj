(ns core2.edn
  (:import java.io.Writer
           [java.time Duration Instant ZonedDateTime]))

(defn duration-reader [s] (Duration/parse s))
(defn instant-reader [s] (Instant/parse s))
(defn zdt-reader [s] (ZonedDateTime/parse s))

(when-not (System/getenv "CORE2_DISABLE_EDN_PRINT_METHODS")
  (defmethod print-dup Duration [^Duration d, ^Writer w]
    (.write w (format "#c2/duration \"%s\"" d)))

  (defmethod print-method Duration [d w]
    (print-dup d w))

  (defmethod print-dup Instant [^Instant i, ^Writer w]
    (.write w (format "#c2/instant \"%s\"" i)))

  (defmethod print-method Instant [i w]
    (print-dup i w))

  (defmethod print-dup ZonedDateTime [^ZonedDateTime zdt, ^Writer w]
    (.write w (format "#c2/zdt \"%s\"" zdt)))

  (defmethod print-method ZonedDateTime [zdt w]
    (print-dup zdt w)))
