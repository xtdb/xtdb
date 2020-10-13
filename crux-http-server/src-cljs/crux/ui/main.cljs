(ns ^:figwheel-hooks crux.ui.main
  (:require
   [crux.ui.events :as events]
   [crux.ui.views :as views]
   [crux.ui.navigation :as navigation]
   [reagent.dom :as rdom]
   [re-frame.core :as rf]))

(defn ^:after-load mount-root
  []
  (rf/clear-subscription-cache!)
  (when-let [section (js/document.getElementById "app")]
    (rdom/unmount-component-at-node section)
    (rdom/render [views/view] section)))

(defn ^:export init
  []
  (navigation/init-routes!)
  (rf/dispatch-sync [::events/inject-metadata "options" :options])
  (rf/dispatch-sync [::events/inject-metadata "results" :meta-results])
  (rf/dispatch-sync [::events/inject-local-storage])
  (mount-root))

(defonce run (init))
