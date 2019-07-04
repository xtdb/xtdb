(ns juxt.crux-ui.frontend.views.comps
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.functions :as vu]
            [garden.core :as garden]))


(defn icon' [icon-name]
  [:i.icon {:key icon-name, :class (str "icon-" (name icon-name))}])


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
   {:cursor :pointer}
   [:&--textual
    {}]
   [:&--active
    {:font-weight 400}]])

(defn button-textual [{:keys [on-click active? text] :as params}]
  [:div {:on-click on-click :class (vu/bem-str :button :textual {:active active?})}
   (if (:icon params)
     [:div.button__icon [icon' (:icon params)]])
   [:span.button__text text]])

