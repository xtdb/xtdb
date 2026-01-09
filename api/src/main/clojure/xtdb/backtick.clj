;; THIRD-PARTY SOFTWARE NOTICE
;;
;; This file is derivative of the `backtick` library, which is licensed under the EPL (version 2.0),
;; and hence this file is also licensed under the terms of that license.
;;
;; Originally accessed at https://github.com/brandonbloom/backtick/blob/0463b49ddb0863653231fc6c922bb124ff5f7d25/src/backtick.clj
;; The EPL 2.0 license is available at https://opensource.org/license/epl-2-0/

(ns xtdb.backtick
  (:require [xtdb.error :as err]))

(defn unquote? [form]
  (and (seq? form) (= (first form) 'clojure.core/unquote)))

(defn unquote-splicing? [form]
  (and (seq? form) (= (first form) 'clojure.core/unquote-splicing)))

(defn quote-fn [form]
  (cond
    (symbol? form) `'~form
    (unquote? form) (second form)
    (unquote-splicing? form) (throw (err/incorrect ::splice-not-in-list "splice not in list" {:form form}))
    (record? form) `'~form
    (coll? form)
      (let [parts (for [x (if (map? form)
                            (apply concat form)
                            form)]
                    (if (unquote-splicing? x)
                      (second x)
                      [(quote-fn x)]))
            cat (doall `(concat ~@parts))]
        (cond
          (vector? form) `(vec ~cat)
          (map? form) `(apply hash-map ~cat)
          (set? form) `(set ~cat)
          (seq? form) `(apply list ~cat)
          :else (throw (err/incorrect ::unknown-coll-type "Unknown collection type" {:form form}))))
    :else `'~form))

