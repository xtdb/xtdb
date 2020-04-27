(ns ^:figwheel-hooks crux.ui.main
  (:require
   [crux.ui.events :as events]
   [crux.ui.views :refer [view]]
   [reagent.dom :as r]
   [re-frame.core :as rf]))

(defn mount-root
  []
  (when-let [section (js/document.getElementById "app")]
    ;; clear subscriptions when figwheel reloads js
    (rf/clear-subscription-cache!)
    (rf/dispatch [::events/inject-metadata "result"])
    (r/render [view] section)))

(defn ^:export init
  []
  (mount-root))

(defn ^:after-load re-render []
  (do (init) true))

(defonce run (init))
