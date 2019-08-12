(ns juxt.crux-ui.frontend.main
  (:require [reagent.core :as r]
            [juxt.crux-ui.frontend.subs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.facade :as views]
            [juxt.crux-ui.frontend.events.facade :as events]
            [clojure.string :as s]
            [juxt.crux-ui.frontend.cookies :as c]))


(def example-query-str
  (s/join "\n"
          ["{:find [e]"
           " :where"
           " [[e :crux.db/id _]]"
           "; options"
           " :full-results? true}"]))

(def ^:private now (js/Date.))

(def default-db
  {:db.query/input           example-query-str
   :db.query/time            {:time/vt now :time/tt now}
   :db.query/input-committed example-query-str
   :db.ui/root-tab           :db.ui.root-tab/query-ui
   :db.ui/output-side-tab    nil ;:db.ui.output-tab/table
   :db.ui/output-main-tab    nil ;:db.ui.output-tab/table
   :db.ui/editor-key         0
   :db.ui.examples/closed?   (c/get :db.ui.examples/closed? false)
   :db.query/key             0
   :db.query/error           nil
   :db.query/result          nil})

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
  (rf/dispatch-sync [:evt.db/init default-db])
  (lookup-gist)
  (mount-root))

;; This is called every time you make a code change
(defn reload! []
  (mount-root))
