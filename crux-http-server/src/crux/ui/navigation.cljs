(ns crux.ui.navigation
  (:require
   [crux.ui.routes :as routes]
   [crux.ui.pages :as pages]
   [re-frame.core :as rf]
   [bidi.bidi :as bidi]
   [pushy.core :as pushy]))

(defn- scroll-top
  []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))

(rf/reg-fx
 ::scroll-top
 (fn [_] (scroll-top)))

(rf/reg-event-fx
 ::update-url
 (fn [{:keys [db]} [_ url]]
   (let [route-data (bidi/match-route routes/routes url)
         handler (:handler route-data)
         query-params {:query-params (str (js/URLSearchParams. js/window.location.search))}
         new-page (get pages/pages handler)]
     (merge
      (when-let [dispatch (:dispatch new-page)]
        {:dispatch dispatch})
      {:db (assoc db :current-page (merge route-data
                                          query-params))
       ::scroll-top _}))))

(defn- dispatch-route!
  [url]
  ;; url arg is passed from pushy
  (rf/dispatch [::update-url url]))

(def history (pushy/pushy dispatch-route! identity))

(defn initialize-routes
  []
  (pushy/start! history))

(rf/reg-fx
 ::pushy
 (fn [value]
   (pushy/set-token! history value)))

;; USAGE
;; (rf/dispatch [:navigate {:page :query}])
;; (rf/dispatch [:navigate {:page :query
;;                          :path-params {:param1 "value"
;;                                        :param2 "value"}
;;                          :query-params "find=...&..."}

(rf/reg-event-fx
 :navigate
 (fn [_ [_ {:keys [page path-params query-params]}]]
   (let [route  (cond-> (apply bidi/path-for routes/routes page (mapcat identity path-params))
                  query-params (str "?" query-params))]
     (if route
       {::pushy route}
       (js/console.warn "No matching page found for " page)))))
