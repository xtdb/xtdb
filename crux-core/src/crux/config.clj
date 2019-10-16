(ns crux.config
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io])
  (:import java.util.Properties))

(def property-types
  {::boolean [boolean? (fn [x]
                         (or (and (string? x) (Boolean/parseBoolean x)) x))]
   ::int [int? (fn [x]
                 (or (and (string? x) (Long/parseLong x)) x))]
   ::nat-int [nat-int? (fn [x]
                         (or (and (string? x) (Long/parseLong x)) x))]
   ::string [string? identity]
   ::module [(fn [m] (s/valid? :crux.node/module m))
             (fn [m] (s/conform :crux.node/module m))]})

(s/def ::type (s/and (s/conformer (fn [x] (or (property-types x) x)))
                     (fn [x] (and (vector? x) (-> x first fn?) (some-> x second fn?)))))

(s/def ::doc string?)
(s/def ::default any?)
(s/def ::required? boolean?)

(defn load-properties [f]
  (let [props (Properties.)]
    (.load props (io/reader f))
    (into {}
          (for [[k v] props]
            [(keyword k) v]))))
