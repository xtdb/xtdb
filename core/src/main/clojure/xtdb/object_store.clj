(ns xtdb.object-store)

(set! *unchecked-math* :warn-on-boxed)

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))
