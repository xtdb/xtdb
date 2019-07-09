(ns juxt.crux-ui.frontend.views.query.examples
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.comps :as comps]))

(def ^:private -sub-examples (rf/subscribe [:subs.query/examples]))


(defn is-gist-link? [s]
  (let [url (js/URL. s)]
    (= "gist.githubusercontent.com" (.-hostname url))))

(defn on-examples-add []
  (let [gh-link (js/prompt "Paste a GitHub gist raw content link")]
    (if (is-gist-link? gh-link)
      (rf/dispatch [:evt.ui/github-examples-request gh-link])
      (js/alert "Please ensure it's a raw gist link"))))


(defn root []
  [:div.examples
   [:div.examples__add {:on-click on-examples-add} "my examples"]
   (for [{ex-title :title} @-sub-examples]
     ^{:key ex-title}
     [:div.examples__item
      [comps/button-textual
       {:on-click #(rf/dispatch [:evt.ui.editor/set-example ex-title])
        :text ex-title}]])])
