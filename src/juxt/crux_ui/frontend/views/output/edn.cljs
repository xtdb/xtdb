(ns juxt.crux-ui.frontend.views.output.edn
  (:require [juxt.crux-ui.frontend.views.codemirror :as cm]))



(defn root [edn]
  (let [fmt (with-out-str (cljs.pprint/pprint edn))]
    [cm/code-mirror fmt {:read-only? true}]))
