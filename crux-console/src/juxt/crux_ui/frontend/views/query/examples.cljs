(ns juxt.crux-ui.frontend.views.query.examples
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.comps :as comps]
            [garden.core :as garden]
            [garden.stylesheet :as gs]))


(def ^:private -sub-examples (rf/subscribe [:subs.query/examples]))

(defn is-gist-link? [s]
  (let [url (js/URL. s)]
    (= "gist.githubusercontent.com" (.-hostname url))))

(defn on-examples-add []
  (let [gh-link (js/prompt "Paste a GitHub gist raw content link")]
    (if (is-gist-link? gh-link)
      (rf/dispatch [:evt.ui/github-examples-request gh-link])
      (js/alert "Please ensure it's a raw gist link"))))

(def q-form-styles
  [:style
    (garden/css
      [:.examples
       {:display :flex
        :font-size :13px}
       [:&__item
        {:padding :8px
         :cursor :pointer}
        [:&:hover
         {:color :black}]]
       [:&__title
        {:padding :8px}]
       [:&__import
        {:margin-left :8px
         :padding :8px}]]
      (gs/at-media {:max-width :1000px}
        [:.examples
         {:display :none}]))])

(defn root []
  [:div.examples
   q-form-styles
   [:div.examples__title "Examples: "]
   (for [{ex-title :title} @-sub-examples]
     ^{:key ex-title}
     [:div.examples__item
      [comps/button-textual
       {:on-click #(rf/dispatch [:evt.ui.editor/set-example ex-title])
        :text ex-title}]])
   [:div.examples__import
    {:on-click on-examples-add}
    "Set my examples"]])

