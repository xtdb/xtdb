(ns juxt.crux-ui.server.pages
  (:require [juxt.crux-ui.server.preloader :as preloader]
            [page-renderer.api :as pr]
            [clojure.java.io :as io]))

(def id #uuid "50005565-299f-4c08-86d0-b1919bf4b7a9")

(def console-assets-frame
  {:title "Crux Console"
   :lang "en"
   :theme-color "hsl(32, 91%, 54%)"
   :og-image "/static/img/crux-logo.svg"
   :link-apple-icon "/static/img/cube-on-white-192.png"
   :link-apple-startup-image "/static/img/cube-on-white-512.png"
   :link-image-src "/static/img/cube-on-white-512.png"
   :service-worker "/service-worker-for-console.js"
   :favicon "/static/img/cube-on-white-120.png"
   :sw-default-url "/console"
   :stylesheet-async
   ["/static/styles/reset.css"
    "/static/styles/react-input-range.css"
    "/static/styles/react-ui-tree.css"
    "/static/styles/codemirror.css"
    "/static/styles/monokai.css"
    "/static/styles/eclipse.css"]
   :script "/static/crux-ui/compiled/main.js"
   :manifest "/static/manifest-console.json"
   :head-tags
   [[:style#_stylefy-constant-styles_]
    [:style#_stylefy-styles_]
    [:meta {:name "google" :content "notranslate"}]]
   :body [:body [:div#app preloader/root]]})

(defn gen-console-page [req]
  (pr/render-page console-assets-frame))

(defn gen-service-worker [req]
  (pr/generate-service-worker console-assets-frame))

(defn q-perf-page [req]
  (pr/render-page
    {:title            "Crux Console"
     :lang             "en"
     :doc-attrs        {:data-scenario :perf-plot}
     :stylesheet-async "/static/styles/reset.css"
     :og-image "/static/img/crux-logo.svg"
     :script      "/static/crux-ui/compiled/main-perf.js"
     :head-tags
     [[:script {:id "plots-data" :type "text/edn"}
       (slurp (io/resource "static/plots-data.edn"))]]
     :body [:body [:div#app preloader/root]]}))

(defn gen-home-page [req]
  (pr/render-page
    {:title "Crux Standalone Demo with HTTP"
     :lang "en"
     :og-image "/static/img/crux-logo.svg"
     :body
      [:body
       [:header
        [:div.nav
         [:div.logo {:style {:opacity "0"}}
          [:a {:href "/"} [:img.logo-img {:src "/static/img/crux-logo.svg"}]]]
         [:div.n0
          [:a {:href "https://juxt.pro/crux/docs/index.html"} [:span.n "Documentation"]]
          [:a {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux"} [:span.n "Community Chat"]]
          [:a {:href "mailto:crux@juxt.pro"} [:span.n "crux@juxt.pro"]]]]]
       [:div {:style {:text-align "center" :width "100%" :margin-top "6em"}}
           [:div.splash {:style {:max-width "25vw" :margin-left "auto" :margin-right "auto"}}
            [:a {:href "/"} [:img.splash-img {:src "/static/img/crux-logo.svg"}]]]

        [:div {:style {:height "4em"}}]
        [:h3 "You should now be able to access this Crux standalone demo node using the HTTP API via localhost:8080"]]

       [:div#app]]}))
