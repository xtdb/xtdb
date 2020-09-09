(ns crux.config
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import java.util.Properties
           (java.io File)
           (java.nio.file Path Paths)
           (java.time Duration)
           (java.util.concurrent TimeUnit)))

(s/def ::boolean
  (s/and (s/conformer (fn [x] (cond (boolean? x) x, (string? x) (Boolean/parseBoolean x), :else x)))
         boolean?))

(s/def ::string string?)

(s/def ::path
  (s/and (s/conformer (fn [fp]
                        (cond
                          (instance? Path fp) fp
                          (instance? File fp) (.toPath ^File fp)
                          (string? fp) (Paths/get fp (make-array String 0)))))
         #(instance? Path %)))

(s/def ::int
  (s/and (s/conformer (fn [x] (cond (int? x) x, (string? x) (Integer/parseInt x), :else x)))
         int?))

(s/def ::nat-int
  (s/and (s/conformer (fn [x] (cond (nat-int? x) x, (string? x) (Integer/parseInt x), :else x)))
         nat-int?))

(s/def ::string-map (s/map-of string? string?))
(s/def ::string-list (s/coll-of string?))

(s/def ::duration
  (s/and (s/conformer (fn [d]
                        (cond
                          (instance? Duration d) d
                          (nat-int? d) (Duration/ofMillis d)
                          (string? d) (Duration/parse d))))
         #(instance? Duration %)))

(s/def ::time-unit
  (s/and (s/conformer (fn [t]
                        (cond
                          (instance? TimeUnit t) t
                          (string? t) (TimeUnit/valueOf (str/upper-case t)))))
         #(instance? TimeUnit %)))

(s/def ::type
  (s/conformer (fn [t]
                 (if (or (s/get-spec t) (s/spec? t))
                   t
                   any?))))

(s/def ::fn fn?)

(s/def ::doc string?)
(s/def ::default any?)
(s/def ::required? boolean?)
