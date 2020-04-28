(ns crux.ui.navigation
  (:require
   [crux.ui.routes :as routes]
   [re-frame.core :as rf]
   [bidi.bidi :as bidi]
   [pushy.core :as pushy]))

(defn- scroll-top
  []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))

(rf/reg-fx
 :scroll-top
 (fn [_] (scroll-top)))

(rf/reg-event-fx
 :update-url
 (fn [{:keys [db]} [_ url]]
   (let [route-data (bidi/match-route routes/routes url)
         handler (:handler route-data)]
     {:db (assoc db :page handler)
      :scroll-top _})))

(defn- dispatch-route!
  [url]
  ;; url arg is passed from pushy
  (rf/dispatch [:update-url url]))

(def history (pushy/pushy dispatch-route! identity))

(defn initialize-routes
  []
  (pushy/start! history))

(rf/reg-fx
 :pushy
 (fn [value]
   (pushy/set-token! history value)))

;; USE
;; (rf/dispatch [:navigate :page])
;; (rf/dispatch [:navigate :page :param "foo-param"])

(rf/reg-event-fx
 :navigate
 (fn [_ [_ page & params]]
   (let [route (apply bidi/path-for routes/routes page params)]
     (if route
       {:pushy route}
       (js/console.warn "No matching page found for " page)))))
