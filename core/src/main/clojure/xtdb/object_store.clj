(ns xtdb.object-store)

(set! *unchecked-math* :warn-on-boxed)

(defn ensure-shared-range-oob-behaviour [^long i ^long len]
  (when (< i 0)
    (throw (IndexOutOfBoundsException. "Negative range indexes are not permitted")))
  (when (< len 1)
    (throw (IllegalArgumentException. "Negative or zero range requests are not permitted"))))

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))
