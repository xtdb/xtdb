(ns crux-ui.main
  (:require [reagent.core :as r]
            [crux-ui.subs]
            [re-frame.core :as rf]
            [crux-ui.views.facade :as views]
            [crux-ui.events.facade]
            [crux-ui.routes :as routes]
            [crux-ui.events.default-db :as d]
            [clojure.string :as s]
            [crux-ui.logging :as log]))



(set! js/window.tojs clj->js)

(defn mount-root []
  (r/render [views/root] (js/document.getElementById "app")))

(defn dispatch-screen-measurements []
  (rf/dispatch
    [:evt.ui/screen-resize
     {:ui.screen/inner-height js/window.innerHeight
      :ui.screen/inner-width js/window.innerWidth}]))

(defn- listen-keyboard-shortcuts [evt]
  (when (and (.-ctrlKey evt) (= 69 (.-keyCode evt)))
    (rf/dispatch [:evt.keyboard/ctrl-e]))
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
  (js/window.addEventListener "keydown" listen-keyboard-shortcuts true)
  (js/window.addEventListener "resize" dispatch-screen-measurements)
  (rf/dispatch-sync [:evt.db/init d/default-db])
  (routes/init)
  (lookup-gist)
  (js/setTimeout dispatch-screen-measurements 50)
  (mount-root))

;; This is called every time you make a code change
(defn reload! []
  (mount-root))
