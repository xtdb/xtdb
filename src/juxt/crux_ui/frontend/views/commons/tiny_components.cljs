(ns juxt.crux-ui.frontend.views.commons.tiny-components
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.functions :as vu]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.svg-icons :as icon]))


(defn icon' [icon-name]
  [:i.icon {:key icon-name, :class (str "icon-" (name icon-name))}])

(defn link-mailto [email]
  [:a {:href (str "mailto:" email)} email])

(defn link-outer [href title]
  [:a {:target "_blank" :href href :title (str "Open " title " in a new tab")} title #_icon/external])

(defn link-box [{:keys [main? luminous? active? svg? round? icon icon-alt on-click class href label attrs] :as params}]
  (let [css-class
        (-> :link
            (vu/bem-str {:main main? :round round? :active active? :luminous luminous?})
            (str class))]
    [:a.link.link--box.g-nolink (merge attrs {:href href :title icon-alt :on-click on-click :class css-class})
     #_(if icon
         (if svg?
           [svg/icon icon {:active? active?}]
           [:div.link__icon {:title icon-alt} [icon' icon icon-alt]]))
     (if label
       [:div.link__label label])]))


(def button-textual-styles
  [:.button
   {:cursor :pointer
    :background :none
    :padding "6px 8px"
    :border-radius :2px
    :user-select :none
    :border "1px solid hsl(0, 0%, 85%)"}
   [:&--textual
    {:border :none
     :background :none}]
   [:&--active
    {:font-weight 400}]])

(defn button-textual [{:keys [on-click active? text] :as params}]
  [:button
   {:type :button :on-click on-click
    :class (vu/bem-str :button :textual {:active active?})}
   (if (:icon params)
     [:div.button__icon [icon' (:icon params)]])
   [:span.button__text text]])

(defn button-bordered [{:keys [on-click active? text] :as params}]
  [:button
   {:type :button :on-click on-click
    :class (vu/bem-str :button {:active active?})}
   (if (:icon params)
     [:div.button__icon [icon' (:icon params)]])
   [:span.button__text text]])

