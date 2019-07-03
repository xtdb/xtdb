(ns juxt.crux-ui.frontend.main
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.subs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.facade :as views]
            [juxt.crux-ui.frontend.events.facade :as events]
            [clojure.string :as s]))


(def example-query-str
  (s/join "\n"
          ["{:find [e]"
           " :where"
           " [[e :crux.db/id _]]"
           "; options"
           " :full-results? true}"]))

(def default-db
  {:db.query/input  example-query-str
   :db.query/input-committed  example-query-str
   :db.ui/output-tab nil ;:db.ui.output-tab/table
   :db.ui/editor-key 0
   :db.query/key    0
   :db.query/error  nil
   :db.query/result nil})


(defn mount-root []
  (r/render [views/root] (js/document.getElementById "app")))

(defn- listen-keyboard-shortcuts [evt]
  (when (and (.-ctrlKey evt) (= 13 (.-keyCode evt)))
    (rf/dispatch [:evt.keyboard/ctrl-enter])))

(defn ^:export init []
  (js/window.addEventListener "keydown" listen-keyboard-shortcuts)
  (rf/dispatch-sync [:evt.db/init default-db])
  (mount-root))

;; This is called every time you make a code change
(defn ^:after-load on-reload []
  (mount-root))
