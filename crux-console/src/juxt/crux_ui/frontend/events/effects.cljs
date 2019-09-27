(ns juxt.crux-ui.frontend.events.effects
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.io.query :as q]
            [juxt.crux-ui.frontend.routes :as routes]
            [juxt.crux-lib.http-functions :as hf]
            [promesa.core :as p]
            [juxt.crux-ui.frontend.cookies :as c]))

(rf/reg-fx
  :fx/query-exec
  (fn [{:keys [raw-input query-analysis] :as query}]
    (if query
      (q/exec query))))

(defn qs [_]
  (q/fetch-stats))

(rf/reg-fx
  :fx/query-stats
  qs)

(rf/reg-fx
  :fx/set-node
  (fn [node-addr]
    (when node-addr
      (q/set-node! (str "//" node-addr))
      (q/ping-status)
      (q/fetch-stats))))

(rf/reg-fx
  :fx.query/history
  (fn [eids]
    (q/fetch-histories eids)))

(rf/reg-fx
  :fx.query/histories-docs
  (fn [eids->histories]
    (q/fetch-histories-docs eids->histories)))

(rf/reg-fx
  :fx/push-query-into-url
  (fn [^js/String query]
    (println :push-query query)
    (if (and query (< (.-length query) 2000))
      (routes/push-query query))))

(rf/reg-fx
  :fx.sys/set-cookie
  (fn [[cookie-name value]]
    (c/set! cookie-name value {:max-age 86400})))

(rf/reg-fx
  :fx.ui/alert
  (fn [message]
    (js/alert message)))

(rf/reg-fx
  :fx.ui/prompt
  (fn [[message event-id]]
    (rf/dispatch [event-id (js/prompt message)])))

(rf/reg-fx
  :fx.ui/toggle-fullscreen
  (fn [_]
    (if js/document.fullscreen
      (js/document.exitFullscreen)
      (^js .requestFullscreen js/document.documentElement))))

(rf/reg-fx
  :fx/set-polling
  (fn [poll-interval-in-seconds]
    (some-> js/window.__console_polling_id js/clearInterval)
    (when poll-interval-in-seconds
      (let [ms (* 1000 poll-interval-in-seconds)
            iid (js/setInterval #(rf/dispatch [:evt.ui.query/poll-tick]) ms)]
        (set! js/window.__console_polling_id iid)))))

(defn grab-gh-gist [gh-link]
  (-> (hf/fetch gh-link)
      (p/catch #(rf/dispatch [:evt.io/gist-err %]))
      (p/then #(rf/dispatch [:evt.io/gist-success (:body %)]))))

(rf/reg-fx
  :fx/get-github-gist
  grab-gh-gist)
