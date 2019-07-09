(ns juxt.crux-ui.frontend.views.query.examples
  (:require [juxt.crux-lib.http-functions :as hf]
            [promesa.core :as p]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.comps :as comps]))

(def ^:private -sub-examples @(rf/subscribe [:subs.query/examples]))

(defn- grab-gh-gist [gh-link]
  (-> (hf/fetch gh-link)
      (p/catch #(rf/dispatch [:evt.io/gist-err %]))
      (p/then #(rf/dispatch [:evt.io/gist-success (:body %)]))))

(def url "https://gist.githubusercontent.com/spacegangster/b68f72e3c81524a71af1f3033ea7507e/raw/572396dec0791500c965fea443b2f26a60f500d4/examples.edn")

(defn is-gist-link? [s]
  (let [url (js/URL. s)]
    (= "gist.githubusercontent.com" (.-hostname url))))

(defn on-examples-add []
  (let [gh-link (js/prompt "Paste a GitHub gist raw content link")]
    (if (is-gist-link? url)
      (grab-gh-gist gh-link)
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
