(ns crux-ui-server.pages
  (:require [crux-ui-server.preloader :as preloader]
            [page-renderer.api :as pr]
            [clojure.java.io :as io])
  (:import (clojure.lang IPersistentMap)))

(def routes-prefix (atom ""))

(defn console-assets-frame ^IPersistentMap
  [^IPersistentMap config]
  (let [routes-prefix (:console/routes-prefix config)]
    {:title                    "Crux Console"
     :lang                     "en"
     :theme-color              "hsl(32, 91%, 54%)"
     :og-image                 (str routes-prefix "/static/img/crux-logo.svg")
     :link-apple-icon          (str routes-prefix "/static/img/cube-on-white-192.png")
     :link-apple-startup-image (str routes-prefix "/static/img/cube-on-white-512.png")
     :link-image-src           (str routes-prefix "/static/img/cube-on-white-512.png")
     :service-worker           (str routes-prefix "/service-worker-for-console.js")
     :favicon                  (str routes-prefix "/static/img/cube-on-white-120.png")
     :doc-attrs                {:data-routes-prefix  routes-prefix
                                :data-crux-base-url  (:console/crux-node-url-base config)
                                :data-crux-http-port (str (:console/crux-http-port config))}
     :sw-default-url           (str routes-prefix "/app")
     :stylesheet-async
                               [(str routes-prefix "/static/styles/reset.css")
                                (str routes-prefix "/static/styles/react-input-range.css")
                                (str routes-prefix "/static/styles/react-ui-tree.css")
                                (str routes-prefix "/static/styles/codemirror.css")
                                (str routes-prefix "/static/styles/monokai.css")
                                (str routes-prefix "/static/styles/eclipse.css")]
     :script                   (str routes-prefix "/static/crux-ui/compiled/main.js")
     :manifest                 (str routes-prefix "/static/manifest-console.json")
     :head-tags
                               [[:style#_stylefy-constant-styles_]
                                [:style#_stylefy-styles_]
                                [:meta {:name "google" :content "notranslate"}]]
     :body                     [:body [:div#app preloader/root]]}))

(defn gen-console-page [req config]
  (pr/render-page (console-assets-frame config)))

(defn gen-service-worker [req config]
  (pr/generate-service-worker (console-assets-frame config)))

(defn q-perf-page [req]
  (pr/render-page
    {:title            "Crux Console"
     :lang             "en"
     :doc-attrs        {:data-scenario :perf-plot}
     :stylesheet-async (str @routes-prefix "/static/styles/reset.css")
     :og-image         (str @routes-prefix "/static/img/crux-logo.svg")
     :script           (str @routes-prefix "/static/crux-ui/compiled/main-perf.js")
     :head-tags        [[:script {:id "plots-data" :type "text/edn"}
                         (slurp (io/resource "static/plots-data.edn"))]]
     :body             [:body [:div#app preloader/root]]}))

(defn gen-home-page [req]
  (pr/render-page
    {:title "Crux Standalone Demo with HTTP"
     :lang "en"
     :og-image (str @routes-prefix "/static/img/crux-logo.svg")
     :body
      [:body
       [:header
        [:div.nav
         [:div.logo {:style {:opacity "0"}}
          [:a {:href (str @routes-prefix "/")}
           [:img.logo-img {:src (str @routes-prefix "/static/img/crux-logo.svg")}]]]
         [:div.n0
          [:a {:href "https://juxt.pro/crux/docs/index.html"} [:span.n "Documentation"]]
          [:a {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux"} [:span.n "Community Chat"]]
          [:a {:href "mailto:crux@juxt.pro"} [:span.n "crux@juxt.pro"]]]]]
       [:div {:style {:text-align "center" :width "100%" :margin-top "6em"}}
           [:div.splash {:style {:max-width "25vw" :margin-left "auto" :margin-right "auto"}}
            [:a {:href (str @routes-prefix "/")}
             [:img.splash-img {:src (str @routes-prefix "/static/img/crux-logo.svg")}]]]

        [:div {:style {:height "4em"}}]
        [:h3 "You should now be able to access this Crux standalone demo node using the HTTP API via localhost:8080"]]

       [:div#app]]}))
