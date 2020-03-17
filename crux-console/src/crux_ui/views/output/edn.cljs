(ns crux-ui.views.output.edn
  (:require [cljs.pprint :as pp]
            [crux-ui.better-printer :as bp]
            [crux-ui.views.codemirror :as cm]
            [clojure.string :as s]))

(defn root [edn]
  [cm/code-mirror (bp/better-printer edn) {:read-only? true}])

(defn simple-print [edn]
  [cm/code-mirror (bp/simple-print edn) {:read-only? true}])
