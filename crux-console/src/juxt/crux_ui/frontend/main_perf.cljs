(ns juxt.crux-ui.frontend.main-perf
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.subs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.facade-perf :as views]
            [juxt.crux-ui.frontend.events.facade :as events]
            [clojure.string :as s]
            [juxt.crux-ui.frontend.events.default-db :as d]
            [juxt.crux-ui.frontend.cookies :as c]))


(set! js/window.tojs clj->js)

(defn mount-root []
  (r/render [views/root] (js/document.getElementById "app")))

(defn- listen-keyboard-shortcuts [evt]
  (when (and (.-ctrlKey evt) (= 13 (.-keyCode evt)))
    (rf/dispatch [:evt.keyboard/ctrl-enter])))

(defn init []
  (js/window.addEventListener "keydown" listen-keyboard-shortcuts)
  (rf/dispatch-sync [:evt.db/init d/default-db])
  (mount-root))

;; This is called every time you make a code change
(defn reload! []
  (mount-root))
