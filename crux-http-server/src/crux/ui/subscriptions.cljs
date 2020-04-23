(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]))

(rf/reg-sub
 ::query-data
 (fn [db _]
   (:query-data db)))
