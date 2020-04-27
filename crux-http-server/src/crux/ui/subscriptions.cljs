(ns crux.ui.subscriptions
  (:require
   [re-frame.core :as rf]))

(rf/reg-sub
 ::metadata
 (fn [db _]
   (:metadata db)))
