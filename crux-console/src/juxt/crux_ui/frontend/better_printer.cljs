(ns juxt.crux-ui.frontend.better-printer
  (:require [cljs.pprint :as pp]
            [clojure.string :as s]))


(defmulti better-printer
  (fn [edn]
    (cond
      (string? edn) ::string
      (map? edn) ::map
      (vector? edn) ::vec
      :else ::default)))

(defn simple-print [edn]
  (with-out-str (pp/pprint edn)))

(defn- print-entry [i [k v]]
  (if (= 0 i)
    (str (pr-str k) "\t" (simple-print v))
    (str " " (pr-str k) "\t" (simple-print v))))

(defmethod better-printer ::map [edn]
  (str "{" (s/join "\n" (map-indexed print-entry edn)) "}"))

(defmethod better-printer ::string [edn-str]
  (pr-str edn-str))

(defmethod better-printer ::vec [edn]
  (with-out-str (pp/pprint edn)))

(defmethod better-printer ::default [edn]
  (with-out-str (pp/pprint edn)))

(comment
  (println
    (better-printer '{:find [e p], :where [[e :crux.db/id _] [e :ticker/price p]]}))
  (println
    (better-printer '[:find [e p], :where [[e :crux.db/id _] [e :ticker/price p]]])))
