(ns ^:figwheel-hooks xtdb.ui.main
  (:require
   [xtdb.ui.events :as events]
   [xtdb.ui.views :as views]
   [xtdb.ui.navigation :as navigation]
   [reagent.dom :as r]
   [re-frame.core :as rf]))

(defn mount-root
  []
  (when-let [section (js/document.getElementById "app")]
    ;; clear subscriptions when figwheel reloads js
    (rf/clear-subscription-cache!)
    (navigation/init-routes!)
    (rf/dispatch [::events/inject-metadata "options" :options])
    (rf/dispatch [::events/inject-metadata "results" :meta-results])
    (rf/dispatch [::events/inject-local-storage])
    (.setTimeout js/window #(r/render [views/view] section) 0))) ;; not ideal

(defn ^:export init
  []
  (mount-root))

(defn ^:after-load re-render []
  (do (init) true))

(defonce run (init))
