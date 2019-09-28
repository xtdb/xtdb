(ns juxt.crux-ui.frontend.views.output.edn
  (:require [cljs.pprint :as pp]
            [juxt.crux-ui.frontend.better-printer :as bp]
            [juxt.crux-ui.frontend.views.codemirror :as cm]
            [clojure.string :as s]))

(defn root [edn]
  [cm/code-mirror (bp/better-printer edn) {:read-only? true}])

(defn simple-print [edn]
  [cm/code-mirror (bp/simple-print edn) {:read-only? true}])
