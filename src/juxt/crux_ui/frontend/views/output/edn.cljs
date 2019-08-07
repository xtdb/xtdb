(ns juxt.crux-ui.frontend.views.output.edn
  (:require [cljs.pprint :refer [pprint]]
            [juxt.crux-ui.frontend.views.codemirror :as cm]))

(defn root [edn]
  (let [fmt (with-out-str (pprint edn))]
    [cm/code-mirror fmt {:read-only? true}]))
