(ns crux.config
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import java.util.Properties
           (java.io File)
           (java.time Duration)
           (java.util.concurrent TimeUnit)))

(s/def ::boolean
  (s/and (s/conformer (fn [x] (cond (boolean? x) x, (string? x) (Boolean/parseBoolean x), :else x)))
         boolean?))

(s/def ::string string?)

(s/def ::file-path
  (s/and (s/conformer (fn [fp]
                        (cond
                          (instance? File fp) (str fp)
                          (string? fp) fp)))
         string?))

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

(s/def ::doc string?)
(s/def ::default any?)
(s/def ::required? boolean?)

(defn load-properties [f]
  (with-open [rdr (io/reader f)]
    (let [props (Properties.)]
      (.load props rdr)
      (into {}
            (for [[k v] props]
              [(keyword k) v])))))

(defn load-edn [f]
  (with-open [rdr (io/reader f)]
    (into {}
          (for [[k v] (edn/read-string (slurp rdr))]
            [(keyword k) v]))))
