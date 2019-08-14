(ns juxt.crux-ui.frontend.main
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.subs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.facade :as views]
            [juxt.crux-ui.frontend.events.facade :as events]
            [juxt.crux-ui.frontend.events.default-db :as d]
            [clojure.string :as s]))



(set! js/window.tojs clj->js)

(defn mount-root []
  (r/render [views/root] (js/document.getElementById "app")))

(defn- listen-keyboard-shortcuts [evt]
  (when (and (.-ctrlKey evt) (= 13 (.-keyCode evt)))
    (rf/dispatch [:evt.keyboard/ctrl-enter])))

(defn lookup-gist []
  (let [search js/location.search
        re-expls #"\?examples-gist="]
    (when (re-find re-expls search)
      (println :examples-gist-link-found (s/replace search re-expls ""))
      (rf/dispatch [:evt.ui/github-examples-request (s/replace search re-expls "")]))))

; (lookup-gist)

(defn init []
  (js/window.addEventListener "keydown" listen-keyboard-shortcuts)
  (rf/dispatch-sync [:evt.db/init d/default-db])
  (lookup-gist)
  (mount-root))

;; This is called every time you make a code change
(defn reload! []
  (mount-root))
