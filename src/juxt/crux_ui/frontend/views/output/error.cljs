(ns juxt.crux-ui.frontend.views.output.error
  (:require [cljs.pprint :refer [pprint]]
            [juxt.crux-ui.frontend.views.codemirror :as cm]
            [garden.core :as garden]))


(def ^:private style
  [:style
   (garden/css
     [:.err-info
      {:padding "8px 16px"
       :height :100%
       :overflow :scroll}
      [:&__header
       {:line-height 1
        :padding "24px 8px 24px"}]
      [:&__title
        {:letter-spacing :0.09em
         :font-size :30px}]
      [:&__subtitle
       {:font-size :24px
        :margin-top :8px}]])])



(defn root [{:keys [query-type err/http-status err/type err/exception] :as err-data}]
  (let [fmt (with-out-str (pprint err-data))]
    [:div.err-info
     style
     [:div.err-info__header
      [:h1.err-info__title http-status " Error"]
      [:h2.err-info__subtitle
        (case type
          :err.type/client "Server considered the query malformed"
          :err.type/server "HTTP Server produced an error while executing the query")]]
     [:div.err-info__raw
       [cm/code-mirror fmt {:read-only? true}]]]))
