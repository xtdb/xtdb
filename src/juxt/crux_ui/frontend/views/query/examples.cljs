(ns juxt.crux-ui.frontend.views.query.examples
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.tiny-components  :as comps]
            [juxt.crux-ui.frontend.svg-icons :as icon]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [reagent.core :as r]))


(def ^:private -sub-examples (rf/subscribe [:subs.query/examples]))

(defn dispatch-examples-close []
  (rf/dispatch [:evt.ui.examples/close]))

(defn- dispatch-example [ex-title]
  (rf/dispatch [:evt.ui.editor/set-example ex-title]))

(defn is-gist-link? [s]
  (let [url (js/URL. s)]
    (= "gist.githubusercontent.com" (.-hostname url))))

(defn on-examples-add []
  (let [gh-link (js/prompt "Paste a GitHub gist raw content link")]
    (if (is-gist-link? gh-link)
      (rf/dispatch [:evt.ui/github-examples-request gh-link])
      (js/alert "Please ensure it's a raw gist link"))))

(def ^:private q-form-styles
  [:style
    (garden/css
      [:.examples
       {:display :flex
        :width :max-content
        :align-items :center
        :font-size :13px}
       [:&__item
        :&__close
        {:padding :8px
         :cursor :pointer}
        [:&:hover
         {:color :black}]]
       [:&__close
        {:line-height 0
         :padding "0 8px"
         :display :inline-flex
         :align-items :center}]
       [:&__title
        {:padding :8px}]
       [:&__import
        {:margin-left :8px
         :padding :8px}]]
      (gs/at-media {:max-width :1000px}
        [:.examples
         {:display :none}]))])

(def ^{:private true :const true} examples-close-text
  "Tap to close examples. You can always get them back by resetting the cookies on this host.")

(defn root []
  (if-let [examples @-sub-examples]
    [:div.examples
     q-form-styles
     [:div.examples__close
      {:title examples-close-text
       :on-click dispatch-examples-close} icon/close]
     [:div.examples__title "Examples: "]
     (for [{ex-title :title} examples]
       ^{:key ex-title}
       [:div.examples__item
        [comps/button-textual
         {:on-click (r/partial dispatch-example ex-title)
          :text     ex-title}]])
     [:div.examples__import
      {:on-click on-examples-add}
      "Set my examples"]]))

