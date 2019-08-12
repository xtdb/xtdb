(ns juxt.crux-ui.frontend.better-printer
  (:require [cljs.pprint :as pp]
            [clojure.string :as s]))

(defmulti better-printer
          (fn [edn]
            (cond
              (map? edn) ::map
              (vector? edn) ::vec
              :else ::default)))

(defn- print-entry [i [k v]]
  (if (= 0 i)
    (str (pr-str k) "\t" (pr-str v))
    (str " " (pr-str k) "\t" (pr-str v))))

(defmethod better-printer ::map [edn]
  (str "{" (s/join "\n" (map-indexed print-entry edn)) "}"))

(defmethod better-printer ::vec [edn]
  (with-out-str (pp/pprint edn)))

(defmethod better-printer ::default [edn]
  (with-out-str (pp/pprint edn)))

(println
  (better-printer '{:find [e p], :where [[e :crux.db/id _] [e :ticker/price p]]}))

(println
  (better-printer '[:find [e p], :where [[e :crux.db/id _] [e :ticker/price p]]]))
