(ns juxt.crux-ui.frontend.views.commons.tiny-components
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.functions :as vu]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.svg-icons :as icon]
            [juxt.crux-ui.frontend.views.style :as s]))


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

(def col-base {:h 197 :s 80 :l 65 :a 0.8})

(def btn-color--cta        (s/hsl col-base))
(def btn-color--inactive   (s/hsl (assoc col-base :a 0.5)))
(def btn-color--cta-hover  (s/hsl (assoc col-base :a 0.9)))
(def btn-color--cta-active (s/hsl (assoc col-base :a 1)))



(def button-styles
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
    {:font-weight 400}]
   [:&--cta.button--inactive
    {:background btn-color--inactive}]
   [:&--cta
    {:background    btn-color--cta
     :border-radius :2px
     :color         "white"
     :cursor        :pointer
     :border        0
     :letter-spacing "0.03em"
     :padding       "8px 14px"}
    [:&:hover
     {:background btn-color--cta-hover}]
    [:&:active
     {:background btn-color--cta-active}]]])

(defn- button [main-mod {:keys [on-click active? text css-mods] :as params}]
  [:button
   {:type :button
    :on-click on-click
    :class (vu/bem-str :button main-mod css-mods {:active active?})}
   (if (:icon params)
     [:div.button__icon [icon' (:icon params)]])
   [:span.button__text text]])

(def button-textual (partial button :textual))
(def button-cta (partial button :cta))
(def button-bordered (partial button :bordered))
